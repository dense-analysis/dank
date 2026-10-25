from __future__ import annotations

import asyncio
import datetime
import fcntl
import hashlib
import json
import logging
import math
import pathlib
import re
import shutil
import tempfile
import uuid
from collections.abc import AsyncIterator
from contextlib import ExitStack, nullcontext
from itertools import batched
from typing import Any, NamedTuple, cast
from urllib.parse import urlsplit

import aiohttp

from dank.config import (
    MEDIA_DOWNLOAD_TYPES,
    HackerNewsSettings,
    RedditSettings,
    RedditSourceConfig,
    Settings,
    SourceAdapter,
    SourceAdapterConfig,
    SourceConfig,
    XSettings,
    load_settings,
)
from dank.html_utils import PAGE_CHALLENGE_PATTERN
from dank.links import is_original_source, is_platform_url, normalized_url
from dank.logging_setup import configure_logging
from dank.model import (
    AssetDiscovery,
    RawPost,
    RedditCommunityDiscovery,
    YouTubeChannelDiscovery,
    YouTubePlaylistCapture,
)
from dank.progress import Progress
from dank.references import reference_rows as post_reference_rows
from dank.runtime import runtime_info
from dank.scrape.assets import download_assets
from dank.scrape.assets.http import (
    thumbnail_observation,
    youtube_thumbnail_urls,
)
from dank.scrape.concurrency import collection_resources
from dank.scrape.hacker_news import (
    scrape_hacker_news,
    scrape_hacker_news_details,
)
from dank.scrape.hacker_news_rss import scrape_hacker_news_rss
from dank.scrape.http import HostCooldownError, RequestLimiter, current_limiter
from dank.scrape.linked_articles import LinkedArticle, scrape_linked_articles
from dank.scrape.metrics import (
    RSS_CONCURRENCY,
    RunMetrics,
    ScrapeOptions,
    SourceMetrics,
    count,
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
from dank.scrape.x.home import scrape_x_for_you
from dank.scrape.x.session import LoginRequiredError
from dank.scrape.youtube import scrape_youtube_feeds
from dank.scrape.youtube.channels import CHANNEL_ID, channel_icon_url
from dank.scrape.youtube.metadata import fetch_video_metadata
from dank.scrape.youtube.playlists import playlist_posts, snapshot_hash
from dank.scrape.youtube.transcripts import fetch_video_transcript
from dank.scrape.zendriver import (
    BrowserConfig,
    BrowserProfileBusyError,
    BrowserSession,
)
from dank.storage.clickhouse import ClickHouseClient, parse_datetime
from dank.thumbnails import thumbnail_suffix
from dank.youtube import VIDEO_ID, discovery_from_post, payload_object
from dank.youtube_references import reference_rows

logger = logging.getLogger(__name__)
# Community avatars should remain small even when post media is unlimited.
MAX_COMMUNITY_ICON_BYTES = 1024 * 1024
# Bound the extra work after a feed run; subsequent runs drain the backlog.
MAX_LINKED_METADATA_PER_RUN = 100
# Limit extra watch-page requests; older items are checked when opened.
MAX_TRANSCRIPTS_PER_RUN = 25


class CollectionBusyError(RuntimeError):
    """Another collector owns an overlapping source or browser profile."""


async def run_scrape(
    settings: Settings,
    *,
    headless: bool = False,
    batch_size: int = 50,
    retry_failed_pages: bool = False,
    x_posts: tuple[str, ...] = (),
    reddit_details: bool = False,
    reddit_linked_articles: bool = False,
    x_details: bool = False,
    hacker_news_details: bool = False,
    target_post: tuple[str, str] | None = None,
    scoped: bool = False,
    exclusive: bool = False,
) -> RunMetrics:
    if target_post is not None:
        selected = tuple(source for source in settings.sources
                         if source.domain == target_post[0])

        if not selected:
            raise ValueError(
                "The target post must belong to the selected sources",
            )

        settings = settings._replace(sources=selected[:1])
        scoped = True

    settings.data_dir.mkdir(parents=True, exist_ok=True)

    with ExitStack() as locks:
        lock = locks.enter_context(
            (settings.data_dir / "collection.lock").open("a"),
        )
        operation = fcntl.LOCK_EX if exclusive or not scoped else fcntl.LOCK_SH

        try:
            fcntl.flock(lock, operation | fcntl.LOCK_NB)

            if operation == fcntl.LOCK_SH:
                directory = settings.data_dir / "collection-locks"
                directory.mkdir(exist_ok=True)

                for resource in sorted(collection_resources(
                    settings, target_post,
                )):
                    name = hashlib.sha256(resource.encode()).hexdigest()
                    resource_lock = locks.enter_context(
                        (directory / name).open("a"),
                    )
                    fcntl.flock(resource_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise CollectionBusyError(
                "Another collection is already using this source or browser",
            ) from error

        return await _run_scrape(
            settings, headless=headless, batch_size=batch_size,
            retry_failed_pages=retry_failed_pages, x_posts=x_posts,
            reddit_details=reddit_details,
            reddit_linked_articles=reddit_linked_articles,
            x_details=x_details, hacker_news_details=hacker_news_details,
            target_post=target_post, scoped=scoped,
        )


YOUTUBE_HOME_URL = "https://www.youtube.com/"


class _YouTubeComparisonCapture(NamedTuple):
    posts: tuple[RawPost, ...]
    run_id: uuid.UUID
    started_at: datetime.datetime
    finished_at: datetime.datetime
    elapsed_ms: int
    outcome: str


def _comparison_timestamp(value: object) -> datetime.datetime:
    if not isinstance(value, str):
        raise ValueError(
            "Comparison timestamps must be timezone-aware ISO text",
        )

    stamp = datetime.datetime.fromisoformat(value)

    if stamp.tzinfo is None:
        raise ValueError("Comparison timestamps must include a timezone")

    return stamp.astimezone(datetime.UTC)


def _comparison_post(entry: object, position: int) -> RawPost:
    if not isinstance(entry, dict):
        raise ValueError("Invalid browser comparison entry")

    row = cast(dict[str, Any], entry)
    raw = payload_object(row.get("raw"))
    discovery = payload_object(raw.get("discovery"))
    kind = row.get("kind")

    if (not isinstance(kind, str) or kind not in {"video", "short", "post"}
            or not isinstance(row.get("raw"), dict)
            or not isinstance(raw.get("discovery"), dict)
            or not isinstance(raw.get("renderer"), dict)
            or raw.get("kind") != kind
            or type(raw.get("schema_version")) is not int
            or raw["schema_version"] != 1
            or not payload_object(raw.get("renderer"))
            or discovery.get("feed_url") != YOUTUBE_HOME_URL
            or discovery.get("feed_kind") != "home"):
        raise ValueError(
            "Browser entries must retain homepage renderer provenance",
        )

    identifier = raw.get("post_id" if kind == "post" else "video_id")
    pattern = r"[A-Za-z0-9_-]{10,128}" if kind == "post" else VIDEO_ID.pattern
    prefix = "post" if kind == "post" else "video"

    if (not isinstance(identifier, str)
            or not re.fullmatch(pattern, identifier)
            or row.get("identity") != prefix + ":" + identifier):
        raise ValueError(
            "Browser comparison identity does not match its payload",
        )

    for value in (row.get("position"), discovery.get("position")):
        if type(value) is not int or value != position:
            raise ValueError(
                "Browser comparison discovery order is inconsistent",
            )

    rank = discovery.get("rank")

    if (type(rank) is not int or not 1 <= rank < 2 ** 32
            or not isinstance(discovery.get("shelf"), str)):
        raise ValueError(
            "Browser comparison discovery rank or shelf is invalid",
        )

    url = (YOUTUBE_HOME_URL + "post/" + identifier if kind == "post" else
           YOUTUBE_HOME_URL + "watch?v=" + identifier)

    return RawPost(
        "youtube.com", prefix + ":" + identifier, url, None,
        _comparison_timestamp(discovery.get("observed_at")), "youtube",
        YOUTUBE_HOME_URL,
        json.dumps(raw, separators=(",", ":"), allow_nan=False),
    )


def _comparison_capture(report: dict[str, Any]) -> _YouTubeComparisonCapture:
    if (type(report.get("schema_version")) is not int
            or report["schema_version"] != 1
            or report.get("feed_url") != YOUTUBE_HOME_URL):
        raise ValueError("Expected a version 1 YouTube homepage comparison")

    browser = payload_object(report.get("browser"))
    outcome = browser.get("outcome")
    entries = browser.get("entries")
    elapsed = browser.get("elapsed_seconds")

    if (not isinstance(report.get("browser"), dict)
            or not isinstance(outcome, str)
            or outcome not in {"completed", "incomplete", "timeout", "failed"}
            or type(browser.get("partial")) is not bool
            or browser["partial"] != (outcome != "completed")
            or not isinstance(entries, list)
            or not isinstance(elapsed, int | float)
            or isinstance(elapsed, bool)
            or not math.isfinite(elapsed) or not 0 <= elapsed <= 3600):
        raise ValueError("Invalid browser comparison outcome or timing")

    if outcome == "completed" and browser.get("stop_reason") != "end of feed":
        raise ValueError(
            "Completed browser comparison must reach the feed end",
        )

    started = _comparison_timestamp(browser.get("started_at"))
    finished = _comparison_timestamp(browser.get("finished_at"))
    values = cast(list[object], entries)

    if finished < started or len(values) > 20000:
        raise ValueError("Invalid browser comparison bounds")

    posts = tuple(_comparison_post(entry, index)
                  for index, entry in enumerate(values, 1))

    appearances = {
        (post.post_id, payload_object(post.payload)["discovery"]["shelf"])
        for post in posts
    }

    if len(appearances) != len(posts):
        raise ValueError(
            "Browser comparison repeats the same shelf appearance",
        )

    if any(not started <= post.scraped_at <= finished for post in posts):
        raise ValueError(
            "Observation timestamps fall outside browser collection",
        )

    identity = json.dumps({
        "feed": YOUTUBE_HOME_URL, "started_at": started.isoformat(),
        "entries": values,
    }, sort_keys=True, separators=(",", ":"), allow_nan=False)
    run_id = uuid.uuid5(
        uuid.NAMESPACE_URL, "dank-youtube-comparison:" + identity,
    )

    return _YouTubeComparisonCapture(
        posts, run_id, started, finished,
        round(float(elapsed) * 1000), outcome,
    )


def _comparison_history(
    settings: Settings, capture: _YouTubeComparisonCapture,
) -> RunMetrics:
    history = RunMetrics(
        runtime_info(), 1, 50, headless=True, limits=settings.scrape,
        options=ScrapeOptions(media_download_types=()),
    )
    history.run_id = capture.run_id
    history.started_at = capture.started_at
    source = SourceMetrics(1, SourceConfig(
        "youtube.com", (), feed_urls=(YOUTUBE_HOME_URL,),
    ))
    source.started_at = capture.started_at
    source.discovered(len(capture.posts), 0)

    if capture.outcome != "completed":
        source.counts["fetch_failures"] = 1
        source.error_type = "YouTubeFeedIncomplete"

    history.sources[1] = source

    return history


def _finish_comparison(
    history: RunMetrics, capture: _YouTubeComparisonCapture,
    error: BaseException | None = None,
) -> None:
    history.finish(error)
    history.finished_at = capture.finished_at
    history.elapsed_ms = capture.elapsed_ms

    for source in history.sources.values():
        source.finished_at = capture.finished_at
        source.elapsed_ms = capture.elapsed_ms
        source.collection_ms = capture.elapsed_ms


async def _store_comparison(
    client: ClickHouseClient, settings: Settings,
    capture: _YouTubeComparisonCapture,
) -> tuple[str, int, bool]:
    await _ensure_history_tables(client)
    existing = await client.fetch_json(
        "SELECT status, posts_saved FROM scrape_runs FINAL "
        "WHERE run_id = %(run_id)s", {"run_id": str(capture.run_id)},
    )

    if existing.rows:
        previous = existing.rows[0]

        if previous["status"] not in {"completed", "partial"}:
            raise ValueError(
                "An interrupted comparison import already exists; inspect its "
                "saved rows before retrying to avoid duplicate raw posts",
            )

        expected = "completed" if capture.outcome == "completed" else "partial"

        if (previous["status"] != expected
                or int(previous["posts_saved"]) != len(capture.posts)):
            raise ValueError(
                "Saved comparison history differs from this report",
            )

        return str(previous["status"]), int(previous["posts_saved"]), True

    schema = pathlib.Path(__file__).parents[1] / "storage/youtube.sql"

    for statement in schema.read_text().split(";"):
        if statement.strip():
            await client.execute(statement)

    history = _comparison_history(settings, capture)
    source = history.sources[1]
    await client.insert_rows("scrape_runs", [history.row(1)])
    await client.insert_rows(
        "scrape_source_runs", [source.row(history.run_id, 1)],
    )

    try:
        for group in batched(capture.posts, history.batch_size):
            saved = await _flush_posts(client, list(group))
            source.counts["posts_saved"] += saved
            source.persisted(saved)
            await _persist_youtube_discoveries(
                client, list(group), str(history.run_id),
            )

        source.collected()
    except BaseException as error:
        _finish_comparison(history, capture, error)
        await _save_history(client, history)
        raise

    _finish_comparison(history, capture)
    await _save_history(client, history)

    return history.status, source.counts["posts_saved"], False


async def store_youtube_comparison(
    settings: Settings, report: dict[str, Any],
) -> dict[str, Any]:
    """Import browser rows; never scrape or replay an interrupted import."""
    capture = _comparison_capture(report)
    started = asyncio.get_running_loop().time()
    status, count_saved, reused = (
        "skipped" if capture.outcome == "completed" else "failed", 0, False,
    )

    if capture.posts:
        settings.data_dir.mkdir(parents=True, exist_ok=True)

        with (settings.data_dir / "collection.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise CollectionBusyError(
                    "Another collection is running; retry this import later",
                ) from None

            async with ClickHouseClient(settings.clickhouse) as client:
                status, count_saved, reused = await _store_comparison(
                    client, settings, capture,
                )

    return {
        "run_id": str(capture.run_id), "status": status,
        "posts_saved": count_saved, "unique_items": len({
            post.post_id for post in capture.posts
        }), "reused": reused, "capture_elapsed_ms": capture.elapsed_ms,
        "import_elapsed_ms": round(
            (asyncio.get_running_loop().time() - started) * 1000,
        ),
    }


async def _run_scrape(
    settings: Settings,
    *,
    headless: bool = False,
    batch_size: int = 50,
    retry_failed_pages: bool = False,
    x_posts: tuple[str, ...] = (),
    reddit_details: bool = False,
    reddit_linked_articles: bool = False,
    x_details: bool = False,
    hacker_news_details: bool = False,
    target_post: tuple[str, str] | None = None,
    scoped: bool = False,
) -> RunMetrics:
    settings = _detail_source_settings(
        settings, reddit_details=reddit_details, x_details=x_details,
        reddit_linked_articles=reddit_linked_articles,
        hacker_news_details=hacker_news_details,
    )

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
    if not settings.sources:
        logger.warning("No sources selected; check sources and --domains.")
        history.finish()

        return history

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
                    allow_browser_signin=False,
                    session_file=(
                        youtube_profile / "youtube-session.json"
                        if settings.youtube.authenticated else None
                    ),
                )
                await _run_source_sessions(
                    settings, clickhouse_client, history, assets_dir,
                    browser_config, reddit_config, youtube_config,
                    retry_failed_pages=retry_failed_pages, x_posts=x_posts,
                    reddit_details=reddit_details, x_details=x_details,
                    reddit_linked_articles=reddit_linked_articles,
                    hacker_news_details=hacker_news_details,
                    target_post=target_post, scoped=scoped,
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

    return history


def _detail_source_settings(
    settings: Settings, *, reddit_details: bool, x_details: bool,
    reddit_linked_articles: bool = False,
    hacker_news_details: bool,
) -> Settings:
    for enabled, domain in (
        (hacker_news_details, "news.ycombinator.com"),
        (reddit_details or reddit_linked_articles, "reddit.com"),
        (x_details, "x.com"),
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
    reddit_linked_articles: bool = False,
    target_post: tuple[str, str] | None = None,
    scoped: bool = False,
) -> None:
    # Public-page recovery needs no saved sign-in state. Give concurrent
    # reader runs separate profiles while preserving the CLI's existing cache.
    recovery_profile = (
        tempfile.TemporaryDirectory(prefix="dank-rss-") if scoped
        else nullcontext(str(settings.data_dir / "rss-browser-profile"))
    )

    with recovery_profile as rss_profile:
        async with (
            aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
            ) as http_client,
            BrowserSession(x_config) as x_session,
            BrowserSession(reddit_config) as reddit_session,
            BrowserSession(youtube_config) as youtube_session,
            BrowserSession(x_config._replace(
                profile_dir=pathlib.Path(rss_profile),
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
                    reddit_linked_articles=reddit_linked_articles,
                    hacker_news_details=hacker_news_details,
                    target_post=target_post, scoped=scoped,
                )
            finally:
                current_page_recovery.reset(token)


async def _ensure_history_tables(client: ClickHouseClient) -> None:
    links_schema = pathlib.Path(__file__).parents[1] / "storage/links.sql"

    for statement in links_schema.read_text().split(";"):
        if statement.strip():
            await client.execute(statement)

    reference_schema = (
        pathlib.Path(__file__).parents[1] / "storage/references.sql"
    )

    for statement in reference_schema.read_text().split(";"):
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
    reddit_linked_articles: bool = False,
    x_details: bool = False,
    hacker_news_details: bool = False,
    target_post: tuple[str, str] | None = None,
    scoped: bool = False,
) -> None:
    sources = iter(enumerate(settings.sources, 1))
    totals_seen: set[str] = set()
    seen_articles: set[str] = set()
    source_locks = {
        domain: asyncio.Lock()
        for domain in (
            "x.com", "reddit.com", "youtube.com", "news.ycombinator.com",
        )
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
                source_locks.get(source.domain),
                retry_failed_pages=retry_failed_pages, x_posts=x_posts,
                reddit_details=reddit_details, x_details=x_details,
                reddit_linked_articles=reddit_linked_articles,
                hacker_news_details=hacker_news_details,
                totals_seen=totals_seen, seen_articles=seen_articles,
                target_post=target_post,
            )

    try:
        await gather_tasks(
            asyncio.create_task(worker())
            for _ in range(history.source_concurrency)
        )

        if not reddit_linked_articles and target_post is None:
            if not scoped or history.linked_video_ids:
                await collect_linked_youtube_metadata(
                    client, http_client,
                    video_ids=(
                        tuple(sorted(history.linked_video_ids))
                        if scoped else ()
                    ),
                )

            await collect_run_youtube_transcripts(
                client, http_client, str(history.run_id),
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
    reddit_linked_articles: bool = False,
    x_details: bool = False,
    hacker_news_details: bool = False,
    target_post: tuple[str, str] | None = None,
    totals_seen: set[str] | None = None,
    seen_articles: set[str] | None = None,
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
            # Same-site browser tabs and composed HN snapshots are serialized.
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
                    reddit_linked_articles=reddit_linked_articles,
                    hacker_news_details=hacker_news_details,
                    totals_seen=totals_seen, seen_articles=seen_articles,
                    target_post=target_post,
                ):
                    playlist_rows: list[dict[str, Any]] = []

                    if batch.youtube_playlists:
                        batch, playlist_rows = (
                            await _prepare_youtube_playlists(client, batch)
                        )

                    stats.discovered(
                        batch.post_count, len(batch.assets),
                        references=batch.reference_count,
                    )
                    stats.pending += (
                        len(batch.communities) + len(batch.youtube_channels)
                    )
                    # Persist this batch before moving on to the next one.
                    saved = await _flush_posts(client, list(batch.posts))
                    history.collected_posts.update(
                        (post.domain, post.post_id) for post in batch.posts
                        if not post.is_reference
                    )
                    history.linked_video_ids.update(
                        str(evidence["video_id"]) for post in batch.posts
                        for evidence in reference_rows(
                            post._asdict(), raw=True,
                        )
                    )
                    await _persist_youtube_discoveries(
                        client, batch.posts, str(history.run_id),
                    )
                    if playlist_rows:
                        # Publish only after membership and canonical details.
                        await client.insert_rows(
                            "youtube_playlists", playlist_rows,
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
    reddit_linked_articles: bool = False,
    x_details: bool = False,
    hacker_news_details: bool = False,
    target_post: tuple[str, str] | None = None,
    totals_seen: set[str] | None = None,
    seen_articles: set[str] | None = None,
) -> AsyncIterator[ScrapeBatch]:
    if target_post is not None:
        async for batch in _refresh_target_post(
            settings, source, clickhouse_client, http_client, session,
            target_post, batch_size=batch_size,
        ):
            yield batch

        return

    if seen_articles is None:
        seen_articles = set()

    if (x_posts or retry_failed_pages or reddit_details or x_details
            or reddit_linked_articles or hacker_news_details):
        async for batch in _retry_saved_posts(
            settings, source, clickhouse_client, http_client, session,
            x_posts=x_posts, batch_size=batch_size,
            reddit_details=reddit_details, x_details=x_details,
            reddit_linked_articles=reddit_linked_articles,
            hacker_news_details=hacker_news_details,
            seen_articles=seen_articles,
        ):
            yield batch

        return

    for adapter in source.collection_adapters:
        logger.info(
            "%s: collecting with %s adapter", source.domain, adapter.kind,
        )

        try:
            async for batch in _discover_adapter_batches(
                settings, source, adapter, clickhouse_client, http_client,
                session, feed_staleness=feed_staleness,
                batch_size=batch_size, totals_seen=totals_seen,
                seen_articles=seen_articles,
            ):
                logger.info(
                    "Discovered batch domain=%s adapter=%s posts=%d "
                    "references=%d assets=%d", source.domain, adapter.kind,
                    batch.post_count, batch.reference_count, len(batch.assets),
                )
                yield batch
        except (aiohttp.ClientError, TimeoutError, HostCooldownError) as error:
            count("fetch_failures")
            logger.warning("%s: %s adapter failed: %s", source.domain,
                           adapter.kind, error)


async def _load_target_post(
    client: ClickHouseClient, target: tuple[str, str],
) -> RawPost | None:
    saved = await client.fetch_json(
        "SELECT domain, post_id, url, post_created_at, scraped_at, source, "
        "request_url, payload FROM raw_posts "
        "WHERE domain = %(domain)s AND post_id = %(post_id)s "
        "ORDER BY scraped_at DESC LIMIT 1",
        {"domain": target[0], "post_id": target[1]},
    )
    if saved.rows:
        row = saved.rows[0]
        return RawPost(
            domain=str(row["domain"]), post_id=str(row["post_id"]),
            url=str(row["url"]),
            post_created_at=parse_datetime(row.get("post_created_at")),
            scraped_at=parse_datetime(row["scraped_at"])
            or datetime.datetime.now(datetime.UTC),
            source=str(row["source"]), request_url=str(row["request_url"]),
            payload=str(row["payload"]),
        )

    return None


async def _refresh_target_post(
    settings: Settings, source: SourceConfig, client: ClickHouseClient,
    http_client: aiohttp.ClientSession, session: BrowserSession,
    target: tuple[str, str], *, batch_size: int,
) -> AsyncIterator[ScrapeBatch]:
    if target[0] != source.domain:
        raise ValueError("The target post must belong to the selected source")

    post = await _load_target_post(client, target)

    if source.domain == "youtube.com":
        async for batch in _refresh_youtube_post(
            client, http_client, target[1], post,
        ):
            yield batch

        return

    if post is None:
        raise ValueError("The target post must already be saved")

    match source.domain:
        case "reddit.com":
            batches = scrape_reddit_details(
                settings.reddit._replace(capture_comments=True),
                [post], session,
            )
        case "x.com":
            batches = scrape_x_posts(settings.x._replace(
                conversation=settings.x.conversation._replace(enabled=True),
            ), [post], session)
        case "news.ycombinator.com":
            batches = scrape_hacker_news_details(
                settings.hacker_news, [post], http_client,
            )
        case _:
            batches = retry_feed_pages(
                http_client, [post], batch_size=batch_size,
            )

    async for batch in _checked_target_batches(batches):
        yield batch


async def _checked_target_batches(
    batches: AsyncIterator[ScrapeBatch],
) -> AsyncIterator[ScrapeBatch]:
    collected = False
    stats = current_source.get()
    failures = stats.counts["fetch_failures"] if stats else 0

    async for batch in batches:
        collected = collected or bool(batch.posts)

        if any(_incomplete_target_capture(post) for post in batch.posts) and (
            stats is None or stats.counts["fetch_failures"] == failures
        ):
            # Some adapters retain failed/partial payloads without throwing.
            count("fetch_failures")

        yield batch
        failures = stats.counts["fetch_failures"] if stats else 0

    if not collected and (
        stats is None or stats.counts["fetch_failures"] == failures
    ):
        count("fetch_failures")


def _incomplete_target_capture(post: RawPost) -> bool:
    fields = {
        "reddit.com": ("detail_capture", "comment_capture"),
        "x.com": ("conversation_capture",),
        "news.ycombinator.com": ("comment_capture",),
    }.get(post.domain, ())
    payload = payload_object(post.payload)

    if fields:
        return any(
            payload_object(payload.get(field)).get("status") != "captured"
            for field in fields
        )

    return payload.get("page_fetch_status") in {"failed", "unsupported"}


async def _refresh_youtube_post(
    client: ClickHouseClient, http_client: aiohttp.ClientSession,
    post_id: str, post: RawPost | None,
) -> AsyncIterator[ScrapeBatch]:
    video_id = post_id.removeprefix("video:")

    if not post_id.startswith("video:") or not VIDEO_ID.fullmatch(video_id):
        raise ValueError("Only individual YouTube videos can be refreshed")

    if post is None:
        known = await client.fetch_json(
            "SELECT video_id FROM youtube_reference_evidence "
            "WHERE video_id = %(id)s UNION ALL "
            "SELECT substring(item_id, 7) AS video_id "
            "FROM youtube_discoveries "
            "WHERE item_id = %(post_id)s LIMIT 1",
            {"id": video_id, "post_id": post_id},
        )

        if not known.rows:
            raise ValueError("The target video must already be saved")

    metadata = await collect_youtube_video_metadata(
        client, http_client, video_id, force=True,
    )
    transcript = await collect_youtube_video_transcript(
        client, http_client, video_id, force=True,
    )

    if metadata["status"] != "captured":
        count("fetch_failures")

    if transcript["status"] == "failed":
        count("fetch_failures")

    if metadata["status"] != "captured":
        return

    payload = payload_object(post.payload) if post is not None else {}
    payload.update(payload_object(metadata["payload"]))
    payload.update(kind=payload.get("kind") or "video", video_id=video_id)
    now = datetime.datetime.now(datetime.UTC)
    refreshed = RawPost(
        domain="youtube.com", post_id=post_id,
        url=f"https://www.youtube.com/watch?v={video_id}",
        post_created_at=post.post_created_at if post else None,
        scraped_at=now, source="youtube",
        request_url=str(metadata["request_url"]), payload=json.dumps(payload),
    )
    yield ScrapeBatch([refreshed], [])


async def _discover_adapter_batches(
    settings: Settings, source: SourceConfig, adapter: SourceAdapterConfig,
    client: ClickHouseClient, http_client: aiohttp.ClientSession,
    session: BrowserSession, *, feed_staleness: datetime.timedelta,
    batch_size: int, totals_seen: set[str] | None, seen_articles: set[str],
) -> AsyncIterator[ScrapeBatch]:
    match adapter.kind:
        case SourceAdapter.HACKER_NEWS:
            batches_iter = _hacker_news_content_batches(
                settings, source, client, http_client, seen=seen_articles,
            )
        case SourceAdapter.YOUTUBE:
            known_ids = await _known_youtube_channel_ids(client)
            saved_playlists = await client.fetch_json(
                "SELECT * FROM youtube_playlists "
                "ORDER BY checked_at DESC LIMIT 1 BY playlist_id",
            )
            known_playlists = {str(row["playlist_id"]): row
                               for row in saved_playlists.rows}
            batches_iter = scrape_youtube_feeds(
                settings, source, session, known_ids=known_ids,
                known_playlists=known_playlists,
            )
        case SourceAdapter.REDDIT:
            batches_iter = await _reddit_listing_batches(
                settings, source, client, http_client, session, totals_seen,
            )
            batches_iter = _reddit_content_batches(
                settings, client, http_client, batches_iter, seen_articles,
            )
        case SourceAdapter.X:
            batches_iter = _x_listing_batches(
                settings.x, source.accounts, session,
            )
        case SourceAdapter.RSS:
            batches_iter = _rss_adapter_batches(
                settings, source, adapter, client, http_client,
                feed_staleness=feed_staleness, batch_size=batch_size,
                seen_articles=seen_articles,
            )

    async for batch in batches_iter:
        yield batch


async def _x_listing_batches(
    settings: XSettings, accounts: tuple[str, ...], session: BrowserSession,
) -> AsyncIterator[ScrapeBatch]:
    if settings.homepage:
        try:
            async for batch in scrape_x_for_you(settings, session):
                yield batch
        except (LoginRequiredError, BrowserProfileBusyError):
            return

    if not accounts and settings.homepage:
        return

    async for batch in scrape_x_accounts(settings, accounts, session):
        yield batch


async def _rss_adapter_batches(
    settings: Settings, source: SourceConfig, adapter: SourceAdapterConfig,
    client: ClickHouseClient, http_client: aiohttp.ClientSession, *,
    feed_staleness: datetime.timedelta, batch_size: int,
    seen_articles: set[str],
) -> AsyncIterator[ScrapeBatch]:
    feeds = adapter.feed_urls

    if source.domain == "news.ycombinator.com" and not feeds:
        feeds = ("https://news.ycombinator.com/rss",)

    feed_urls = await _source_feed_urls(
        client, source._replace(feed_urls=feeds), feed_staleness,
    )
    stats = current_source.get()

    if stats is not None:
        stats.feed_urls = tuple(dict.fromkeys((*stats.feed_urls, *feed_urls)))
        stats.skipped = not feed_urls and not stats.counts["posts_queued"]

    if not feed_urls:
        logger.warning(
            "%s: no usable feeds; source skipped RSS adapter", source.domain,
        )

        return

    if source.domain == "news.ycombinator.com":
        batches_iter = _hacker_news_content_batches(
            settings, source, client, http_client, seen=seen_articles,
            batches=scrape_hacker_news_rss(
                http_client, feed_urls, batch_size=batch_size,
                max_entries_per_feed=settings.max_entries_per_feed,
            ),
        )
    else:
        batches_iter = scrape_feed_batches(
            http_client, domain=source.domain, feed_urls=feed_urls,
            batch_size=batch_size,
            keep_feed_on_fetch_failure=settings.keep_feed_on_fetch_failure,
            max_entries_per_feed=settings.max_entries_per_feed,
        )

    async for batch in batches_iter:
        yield batch


async def _hacker_news_content_batches(
    settings: Settings, source: SourceConfig,
    client: ClickHouseClient, http_client: aiohttp.ClientSession, *,
    batches: AsyncIterator[ScrapeBatch] | None = None,
    seen: set[str] | None = None,
) -> AsyncIterator[ScrapeBatch]:
    if seen is None:
        seen = set()

    if batches is None:
        batches = scrape_hacker_news(settings.hacker_news, source, http_client)

    async for batch in batches:
        # Save listing/metadata first; a slow publisher cannot erase discovery.
        yield batch
        articles = _hacker_news_article_links(batch.posts, seen)

        if not articles:
            continue

        async for linked_batch in _linked_article_batches(
            client, http_client, articles, seen,
        ):
            yield linked_batch


async def _linked_article_batches(
    client: ClickHouseClient, http_client: aiohttp.ClientSession,
    articles: dict[str, LinkedArticle], seen: set[str],
) -> AsyncIterator[ScrapeBatch]:
    if not articles:
        return

    known = await _known_link_destinations(client, list(articles))
    originals = {
        url: row for url, row in known.items()
        if is_original_source(str(row["domain"]), str(row["url"]))
    }
    pending = [
        article._replace(identity=(
            str(originals[url]["domain"]), str(originals[url]["post_id"]),
        ) if url in originals else None)
        for url, article in articles.items()
        if not is_platform_url(url) and not (
            url in originals and str(originals[url].get("html") or "").strip()
        )
    ]
    logger.info(
        "Linked articles: selected=%d, reused=%d, pending=%d",
        len(articles), sum(
            bool(str(row.get("html") or "").strip())
            for row in originals.values()
        ), len(pending),
    )

    async for linked_batch in scrape_linked_articles(
        http_client, pending, batch_size=10,
    ):
        seen.update(
            url for post in linked_batch.posts
            if (url := normalized_url(post.url))
        )
        yield linked_batch


def _collection_origin(post: RawPost) -> dict[str, str]:
    origin = {
        "domain": post.domain, "post_id": post.post_id,
        "source": post.source, "url": post.url,
        "observed_at": post.scraped_at.isoformat(),
    }
    community = payload_object(post.payload).get("subreddit")

    if isinstance(community, str) and community:
        origin["community"] = community

    return origin


async def _reddit_content_batches(
    settings: Settings, client: ClickHouseClient,
    http_client: aiohttp.ClientSession, batches: AsyncIterator[ScrapeBatch],
    seen: set[str],
) -> AsyncIterator[ScrapeBatch]:
    articles: dict[str, LinkedArticle] = {}

    async for batch in batches:
        # Persist every listing before slow pages can delay other communities.
        yield batch

        if settings.reddit.collect_linked_articles:
            articles.update(_reddit_article_links(batch.posts, seen))

    async for linked_batch in _linked_article_batches(
        client, http_client, articles, seen,
    ):
        yield linked_batch


def _reddit_article_links(
    posts: list[RawPost], seen: set[str],
) -> dict[str, LinkedArticle]:
    links: dict[str, LinkedArticle] = {}

    for post in posts:
        payload = payload_object(post.payload)
        value = payload.get("url")
        url = normalized_url(value) if isinstance(value, str) else None

        if (payload.get("is_self") is True or not url or url in seen
                or url == normalized_url(post.url) or is_platform_url(url)):
            continue

        host = (urlsplit(url).hostname or "").lower()

        if any(host == domain or host.endswith("." + domain) for domain in (
            "reddit.com", "redd.it", "redditmedia.com", "redditstatic.com",
            "youtube.com", "youtu.be", "youtube-nocookie.com", "vimeo.com",
            "tiktok.com", "instagram.com", "facebook.com", "fb.watch",
            "x.com", "twitter.com", "twitch.tv",
        )):
            continue

        seen.add(url)
        links[url] = LinkedArticle(
            url, str(payload.get("title") or ""),
            origin=_collection_origin(post),
        )

    return links


def _hacker_news_article_links(
    posts: list[RawPost], seen: set[str],
) -> dict[str, LinkedArticle]:
    links: dict[str, LinkedArticle] = {}

    for post in posts:
        payload = payload_object(post.payload)
        listing = payload_object(payload.get("hckrnews"))
        story = payload_object(payload.get("story"))
        rss = payload_object(payload.get("rss"))
        url = normalized_url(str(
            listing.get("link") or story.get("url") or rss.get("url") or "",
        ))

        if url and url not in seen and url != normalized_url(post.url):
            seen.add(url)
            links[url] = LinkedArticle(
                url, str(listing.get("link_text") or story.get("title")
                         or rss.get("title") or ""),
                origin=_collection_origin(post),
            )

    return links


async def _known_link_destinations(
    client: ClickHouseClient, urls: list[str],
) -> dict[str, dict[str, Any]]:
    result = await client.fetch_json(
        "SELECT u.url AS lookup_url, p.domain AS domain, "
        "p.post_id AS post_id, p.url AS url, p.html AS html, "
        "p.source AS source FROM post_urls AS u FINAL "
        "INNER JOIN (SELECT domain, post_id, url, html, source, updated_at "
        "FROM posts "
        "FINAL) AS p ON p.domain = u.domain AND p.post_id = u.post_id "
        "AND u.observed_at <= p.updated_at "
        "WHERE u.url IN %(urls)s "
        "ORDER BY (p.domain = domainWithoutWWW(p.url)) DESC, "
        "notEmpty(trimBoth(p.html)) DESC, "
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
            "notEmpty(trimBoth(html)) DESC, "
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


def _playlist_row(capture: YouTubePlaylistCapture) -> dict[str, Any]:
    metadata = capture.metadata

    return {
        **{key: metadata.get(key, "") for key in (
            "playlist_id", "title", "url", "description", "thumbnail_url",
            "channel_id", "channel_name", "channel_url",
            "sort_order", "payload",
        )},
        "reported_count": metadata.get("reported_count"),
        "entry_count": len(capture.entries),
        "snapshot_hash": (
            snapshot_hash(metadata, capture.entries)
            if capture.status in {"complete", "partial"} else ""
        ),
        "observed_at": capture.checked_at, "checked_at": capture.checked_at,
        "status": capture.status, "error_type": capture.error_type,
    }


async def _prepare_youtube_playlists(
    client: ClickHouseClient, batch: ScrapeBatch,
) -> tuple[ScrapeBatch, list[dict[str, Any]]]:
    previous_rows = await client.fetch_json(
        "SELECT * FROM youtube_playlists WHERE playlist_id IN %(ids)s "
        "ORDER BY checked_at DESC LIMIT 1 BY playlist_id",
        {"ids": tuple(item.playlist_id for item in batch.youtube_playlists)},
    )
    previous = {str(row["playlist_id"]): row for row in previous_rows.rows}
    posts = list(batch.posts)
    metadata_rows: list[dict[str, Any]] = []

    for capture in batch.youtube_playlists:
        old = previous.get(capture.playlist_id)

        if capture.status == "not_collected" and old:
            continue

        row = _playlist_row(capture)
        # A count gap is distinct from a stopped continuation chain: YouTube's
        # fully observed visible order must keep updating despite that gap.
        preserve = bool(old and (
            (capture.status != "complete" and not capture.exhausted
             and old["snapshot_hash"])
            or old["snapshot_hash"] == row["snapshot_hash"]
        ))

        if preserve and old is not None:
            row = {**old, "checked_at": capture.checked_at,
                   "status": capture.status, "error_type": capture.error_type}
            row["observed_at"] = parse_datetime(old["observed_at"])
        elif row["snapshot_hash"]:
            # Insert all occurrences before publishing the snapshot pointer.
            rows = [{**entry, "playlist_id": capture.playlist_id,
                     "snapshot_hash": row["snapshot_hash"]}
                    for entry in capture.entries]

            for chunk in batched(rows, 1000):
                await client.insert_rows(
                    "youtube_playlist_entries", list(chunk),
                )

            posts.extend(playlist_posts(capture))

        metadata_rows.append(row)

        if capture.status in {"partial", "failed"}:
            count("fetch_failures")
            stats = current_source.get()

            if stats is not None:
                stats.error_type = "YouTubePlaylistIncomplete"

    return batch._replace(posts=posts, youtube_playlists=()), metadata_rows


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
    reddit_linked_articles: bool = False,
    x_details: bool = False,
    hacker_news_details: bool = False,
    seen_articles: set[str] | None = None,
) -> AsyncIterator[ScrapeBatch]:
    if reddit_linked_articles:
        batches = _saved_reddit_article_batches(
            settings, source, client, http_client,
            seen_articles if seen_articles is not None else set(),
        )
    elif hacker_news_details:
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
        batches = _reddit_content_batches(
            settings, client, http_client,
            scrape_reddit_details(settings.reddit, posts, session),
            seen_articles if seen_articles is not None else set(),
        )
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


async def _saved_reddit_article_batches(
    settings: Settings, source: SourceConfig, client: ClickHouseClient,
    http_client: aiohttp.ClientSession, seen: set[str],
) -> AsyncIterator[ScrapeBatch]:
    if not settings.reddit.collect_linked_articles:
        logger.info("Reddit publisher collection is disabled in configuration")

        return

    posts = await _load_reddit_link_posts(client, source, settings.reddit)
    async for batch in _linked_article_batches(
        client, http_client, _reddit_article_links(posts, seen), seen,
    ):
        yield batch


async def _load_reddit_link_posts(
    client: ClickHouseClient, source: SourceConfig, settings: RedditSettings,
) -> list[RawPost]:
    communities = [
        urlsplit(url).path.split("/")[2].lower() for url in source.feed_urls
    ]
    result = await client.fetch_json(
        "SELECT * FROM (SELECT domain, post_id, url, post_created_at, "
        "scraped_at, source, request_url, payload, "
        "lowerUTF8(JSONExtractString(payload, 'subreddit')) AS community "
        "FROM raw_posts WHERE source = 'reddit' AND domain = 'reddit.com' "
        "AND community IN %(communities)s "
        "ORDER BY scraped_at DESC LIMIT 1 BY post_id) "
        "WHERE NOT JSONExtractBool(payload, 'is_self') "
        "AND JSONExtractString(payload, 'url') != '' "
        "ORDER BY scraped_at DESC LIMIT %(limit)s BY community",
        {"communities": communities, "limit": settings.scrape.max_posts},
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


async def _compose_hacker_news_captures(
    client: ClickHouseClient, posts: list[RawPost],
) -> list[RawPost]:
    ids = [post.post_id for post in posts if post.source == "hacker_news"]

    if not ids:
        return posts

    result = await client.fetch_json(
        "SELECT post_id, payload, post_created_at FROM raw_posts "
        "WHERE domain = 'news.ycombinator.com' AND post_id IN %(ids)s "
        "ORDER BY scraped_at DESC LIMIT 1 BY post_id", {"ids": ids},
    )
    previous = {str(row["post_id"]): row for row in result.rows}
    composed: list[RawPost] = []

    for post in posts:
        if post.source != "hacker_news":
            composed.append(post)
            continue

        old = previous.get(post.post_id, {})
        prior = payload_object(old.get("payload"))
        incoming = payload_object(post.payload)
        payload = prior | incoming
        created = post.post_created_at

        if prior.get("hckrnews") and not incoming.get("hckrnews"):
            created = parse_datetime(old.get("post_created_at")) or created

        post = post._replace(
            payload=json.dumps(payload, ensure_ascii=False),
            post_created_at=created,
        )
        previous[post.post_id] = post._asdict()
        composed.append(post)

    return composed


async def _flush_posts(
    clickhouse_client: ClickHouseClient,
    pending_posts: list[RawPost],
) -> int:
    if not pending_posts:
        return 0

    pending_posts[:] = await _compose_hacker_news_captures(
        clickhouse_client, pending_posts,
    )
    count = len(pending_posts)
    # is_reference is a computed storage column, not part of the raw payload.
    await clickhouse_client.insert_rows(
        "raw_posts",
        [{key: value for key, value in post._asdict().items()
          if key != "is_reference"} for post in pending_posts],
    )
    post_evidence = [link for post in pending_posts
                     for link in post_reference_rows(post._asdict(), raw=True)]

    if post_evidence:
        await clickhouse_client.insert_rows(
            "post_reference_evidence", post_evidence,
        )

    evidence = [link for post in pending_posts
                for link in reference_rows(post._asdict(), raw=True)]

    if evidence:
        await clickhouse_client.insert_rows(
            "youtube_reference_evidence", evidence,
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
    reddit_linked_articles: bool = False,
    x_details: bool = False,
    hacker_news_details: bool = False,
    retry_youtube_thumbnails_only: bool = False,
    youtube_thumbnail_ids: tuple[str, ...] = (),
    youtube_metadata_only: bool = False,
    youtube_video_ids: tuple[str, ...] = (),
) -> None:
    settings = load_settings(path)
    configure_logging(settings.logging, component="scrape")

    if youtube_metadata_only:
        asyncio.run(run_youtube_metadata(
            settings, video_ids=youtube_video_ids,
        ))

        return

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
        reddit_linked_articles=reddit_linked_articles,
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


async def collect_youtube_video_metadata(
    client: ClickHouseClient, http_client: aiohttp.ClientSession,
    video_id: str, *, force: bool = False,
) -> dict[str, Any]:
    """Persist one provider observation, respecting success/failure caches."""
    if not VIDEO_ID.fullmatch(video_id):
        raise ValueError("Invalid YouTube video ID")

    previous = await client.fetch_json(
        "SELECT * FROM youtube_video_metadata WHERE video_id = %(id)s "
        "ORDER BY observed_at DESC LIMIT 1", {"id": video_id},
    )
    now = datetime.datetime.now(datetime.UTC)

    if previous.rows:
        row = previous.rows[0]
        retry_after = parse_datetime(row["retry_after"])

        if not force and retry_after and retry_after > now:
            return {**row, "cached": True}

    row = await fetch_video_metadata(http_client, video_id)
    await client.insert_rows("youtube_video_metadata", [row])

    return {**row, "cached": False}


async def collect_linked_youtube_metadata(
    client: ClickHouseClient, http_client: aiohttp.ClientSession,
    *, video_ids: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    """Enrich verified links or explicitly selected known video IDs."""
    if any(not VIDEO_ID.fullmatch(item) for item in video_ids):
        raise ValueError("Invalid YouTube video ID")

    selected = " AND known.video_id IN %(ids)s" if video_ids else ""
    known = (
        "SELECT video_id FROM youtube_reference_evidence GROUP BY video_id"
    )

    if video_ids:
        known += (
            " UNION DISTINCT SELECT substring(item_id, 7) AS video_id "
            "FROM youtube_discoveries WHERE startsWith(item_id, 'video:')"
        )

    pending = await client.fetch_json(
        "SELECT known.video_id AS video_id FROM (" + known + ") AS known "
        "LEFT JOIN (SELECT v.video_id AS video_id, "
        "argMax(v.retry_after, v.observed_at) AS retry_after "
        "FROM youtube_video_metadata AS v GROUP BY v.video_id) AS m "
        "ON known.video_id = m.video_id "
        "WHERE (m.video_id = '' OR m.retry_after <= now64(3))" + selected +
        " ORDER BY m.retry_after ASC, known.video_id ASC LIMIT %(limit)s",
        {"ids": list(dict.fromkeys(video_ids)),
         "limit": MAX_LINKED_METADATA_PER_RUN},
    )
    queue = iter(str(row["video_id"]) for row in pending.rows)
    outcomes: list[dict[str, Any]] = []

    async def worker() -> None:
        for video_id in queue:
            outcome = await collect_youtube_video_metadata(
                client, http_client, video_id,
            )
            outcomes.append(outcome)
            logger.info(
                "YouTube metadata %s: %s", video_id, outcome["status"],
            )

    # Remain polite even when the generic HTTP limits are configured higher.
    await gather_tasks(asyncio.create_task(worker()) for _ in range(2))

    if outcomes:
        logger.info(
            "YouTube metadata: %d captured; %d unavailable/failed",
            sum(row["status"] == "captured" for row in outcomes),
            sum(row["status"] != "captured" for row in outcomes),
        )

    return outcomes


async def run_youtube_metadata(
    settings: Settings, *, video_ids: tuple[str, ...] = (),
) -> None:
    """Lightweight CLI backfill without launching browsers or feed scrapes."""
    async with (
        ClickHouseClient(settings.clickhouse) as client,
        aiohttp.ClientSession() as http_client,
    ):
        schema = pathlib.Path(__file__).parents[1] / "storage/youtube.sql"

        for statement in schema.read_text().split(";"):
            if statement.strip():
                await client.execute(statement)

        token = current_limiter.set(RequestLimiter(settings.scrape))

        try:
            await collect_linked_youtube_metadata(
                client, http_client, video_ids=video_ids,
            )
        finally:
            current_limiter.reset(token)


async def collect_youtube_video_transcript(
    client: ClickHouseClient, http_client: aiohttp.ClientSession,
    video_id: str, *, force: bool = False,
) -> dict[str, Any]:
    """Reuse saved captions and negative caches; persist only in the runner."""
    if not VIDEO_ID.fullmatch(video_id):
        raise ValueError("Invalid YouTube video ID")

    previous = await client.fetch_json(
        "SELECT * FROM youtube_transcripts WHERE video_id = %(id)s "
        "ORDER BY observed_at DESC LIMIT 1", {"id": video_id},
    )

    if previous.rows:
        row = previous.rows[0]
        retry_after = parse_datetime(row["retry_after"])

        now = datetime.datetime.now(datetime.UTC)

        if row["status"] == "captured" or (
            not force and retry_after and retry_after > now
        ):
            return {**row, "cached": True}

    row = await fetch_video_transcript(http_client, video_id)
    await client.insert_rows("youtube_transcripts", [row])

    return {**row, "cached": False}


async def collect_run_youtube_transcripts(
    client: ClickHouseClient, http_client: aiohttp.ClientSession, run_id: str,
) -> None:
    """Only enrich this run's discoveries, never scan the old-video backlog."""
    pending = await client.fetch_json(
        "SELECT DISTINCT substring(d.item_id, 7) AS video_id "
        "FROM youtube_discoveries AS d LEFT JOIN "
        "(SELECT video_id, argMax(status, observed_at) AS status, "
        "argMax(retry_after, observed_at) AS retry_after "
        "FROM youtube_transcripts GROUP BY video_id) AS t "
        "ON substring(d.item_id, 7) = t.video_id "
        "WHERE d.run_id = %(run)s AND startsWith(d.item_id, 'video:') "
        "AND d.feed_kind != 'playlist' "
        "AND (t.video_id = '' OR (t.status != 'captured' "
        "AND t.retry_after <= now64(3))) ORDER BY video_id LIMIT %(limit)s",
        {"run": run_id, "limit": MAX_TRANSCRIPTS_PER_RUN},
    )

    for item in pending.rows:
        row = await collect_youtube_video_transcript(
            client, http_client, str(item["video_id"]),
        )
        logger.info(
            "YouTube transcript %s: %s", row["video_id"], row["status"],
        )

        if row["status"] == "failed":
            # A blocked provider must not trigger a run of failed requests.
            logger.warning(
                "Stopping transcript collection after a failed request",
            )
            break

        if not row["cached"]:
            await asyncio.sleep(2)


def compare_youtube_home_main() -> None:
    """Run both homepage collectors together and save inspectable evidence."""
    import argparse
    import math

    from dank.scrape.youtube.comparison import compare_home

    parser = argparse.ArgumentParser(
        prog="youtube-compare",
        description="Compare browser and yt-dlp recommendations together.",
    )
    parser.add_argument("--config", default="config.toml")
    parser.add_argument(
        "--timeout", type=float, default=240,
        help="Maximum seconds per method, up to 240 (default: 240)",
    )
    args = parser.parse_args()

    if not math.isfinite(args.timeout) or not 0 < args.timeout <= 240:
        parser.error("--timeout must be positive and no greater than 240")

    settings = load_settings(args.config)
    configure_logging(settings.logging, component="youtube-compare")
    report = asyncio.run(compare_home(settings, timeout_seconds=args.timeout))
    path = save_youtube_comparison(settings, report)
    print(f"YouTube comparison saved: {path}", flush=True)


def save_youtube_comparison(
    settings: Settings, report: dict[str, Any],
) -> pathlib.Path:
    """Persist a collector comparison without cookies or vault credentials."""
    directory = settings.data_dir / "comparisons"
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    path = directory / f"youtube-home-{stamp}.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    path.chmod(0o600)
    return path
