from __future__ import annotations

import asyncio
import datetime
import json
import logging
import pathlib
import re
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from dank.config import (
    BrowserScrapeSettings,
    BrowserSettings,
    ClickHouseSettings,
    LoggingSettings,
    RedditSettings,
    Settings,
    SourceConfig,
    XSettings,
)
from dank.html_utils import PAGE_CHALLENGE_PATTERN
from dank.model import AssetDiscovery, RawAsset, RawPost
from dank.runtime import runtime_info
from dank.scrape.metrics import RunMetrics, ScrapeOptions
from dank.scrape.rss import FetchedText
from dank.scrape.runner import (
    _collect_source,  # pyright: ignore[reportPrivateUsage]
    _discover_source_batches,  # pyright: ignore[reportPrivateUsage]
    _process_batches,  # pyright: ignore[reportPrivateUsage]
    filter_settings_sources,
    run_scrape,
)
from dank.scrape.types import ScrapeBatch, ScrapeTotals
from dank.storage.clickhouse import QueryResult


def _make_settings() -> Settings:
    return Settings(
        clickhouse=ClickHouseSettings(
            host='localhost',
            port=8123,
            database='dank',
            username='default',
            password='',
            secure=False,
            use_http=True,
        ),
        x=XSettings(
            email='x@example.com',
            username='x-user',
            password='secret',
            scrape=BrowserScrapeSettings(max_posts=200),
        ),
        data_dir=pathlib.Path('data'),
        max_asset_bytes=None,
        feed_staleness_days=14,
        sources=(
            SourceConfig(domain='x.com', accounts=('a',)),
            SourceConfig(domain='example.com', accounts=()),
            SourceConfig(domain='news.ycombinator.com', accounts=()),
        ),
        browser=BrowserSettings(
            executable_path=None,
            connection_timeout=None,
            connection_max_tries=None,
        ),
        email=None,
        logging=LoggingSettings(
            file_path=pathlib.Path('dank.log'),
            level='INFO',
        ),
    )


def test_filter_settings_sources_keeps_matching_domains() -> None:
    settings = _make_settings()
    filtered = filter_settings_sources(
        settings,
        re.compile(r'^x\.com$|^example\.com$'),
    )

    assert [source.domain for source in filtered.sources] == [
        'x.com',
        'example.com',
    ]


def test_filter_settings_sources_keeps_no_matches() -> None:
    settings = _make_settings()
    filtered = filter_settings_sources(settings, re.compile(r'^no-match$'))

    assert filtered.sources == ()


async def test_scrape_totals_distinguish_available_and_unavailable_assets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pathlib.Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    now = datetime.datetime.now(datetime.UTC)
    posts = [
        RawPost("example.test", "1", "https://example.test/1", now, now,
                "rss", "https://example.test/feed", "{}"),
    ]
    discoveries = [
        AssetDiscovery("rss", "example.test", "1", url, "image", None)
        for url in ("https://example.test/ok.jpg", "https://example.test/fail.jpg")
    ]
    results = [
        RawAsset("example.test", "1", item.url, "image", now, "rss", path)
        for item, path in zip(discoveries, ("data/ok.jpg", ""), strict=True)
    ]
    saved: dict[str, list[dict[str, Any]]] = {}

    class _Client:
        async def insert_rows(
            self, table: str, rows: list[dict[str, Any]],
        ) -> None:
            saved.setdefault(table, []).extend(rows)

    async def download(*args: Any, **kwargs: Any) -> list[RawAsset]:
        return results

    monkeypatch.setattr("dank.scrape.runner.download_assets", download)
    queue: asyncio.Queue[ScrapeBatch | None] = asyncio.Queue()
    await queue.put(ScrapeBatch(posts, discoveries))
    await queue.put(None)
    totals = await _process_batches(
        queue, cast(Any, _Client()), cast(Any, None),
        assets_dir=tmp_path, browser_profile_dir=tmp_path,
        max_asset_bytes=None, batch_size=50,
    )

    assert totals == ScrapeTotals(1, 1, 1)
    assert len(saved["raw_posts"]) == 1
    assert len(saved["raw_assets"]) == 2
    assert "1 files available; 1 not downloaded" in caplog.text


async def test_failed_database_write_does_not_report_saved_posts(
    tmp_path: pathlib.Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO)
    now = datetime.datetime.now(datetime.UTC)

    class _Client:
        async def insert_rows(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("database unavailable")

    queue: asyncio.Queue[ScrapeBatch | None] = asyncio.Queue()
    await queue.put(ScrapeBatch([
        RawPost("example.test", "1", "https://example.test/1", now, now,
                "rss", "https://example.test/feed", "{}"),
    ], []))
    await queue.put(None)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await _process_batches(
            queue, cast(Any, _Client()), cast(Any, None),
            assets_dir=tmp_path, browser_profile_dir=tmp_path,
            max_asset_bytes=None, batch_size=50,
        )

    assert "Saved" not in caplog.text


async def test_source_without_feeds_is_reported_as_skipped(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _Cache:
        async def fetch_json(self, *args: Any) -> QueryResult:
            return QueryResult([])

    async def discover(*args: Any) -> list[Any]:
        return []

    monkeypatch.setattr("dank.scrape.runner.fetch_feed_links", discover)
    settings = _make_settings()
    source = SourceConfig("missing.test", ())
    batches = [
        batch async for batch in _discover_source_batches(
            settings, source, cast(Any, _Cache()), cast(Any, None),
            cast(Any, None), feed_staleness=datetime.timedelta(days=14),
            batch_size=50,
        )
    ]

    assert batches == []
    assert "missing.test: no usable feeds; source skipped" in caplog.text


async def test_no_selected_sources_warns_without_connecting(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    def unexpected_connection(*args: Any) -> None:
        raise AssertionError("No database connection expected")

    monkeypatch.setattr(
        "dank.scrape.runner.ClickHouseClient", unexpected_connection,
    )
    await run_scrape(_make_settings()._replace(sources=()))

    assert "No sources selected" in caplog.text


async def test_failed_page_mode_uses_saved_entries_without_feed_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.datetime(2026, 10, 15, tzinfo=datetime.UTC)
    source = SourceConfig("example.com", (), ("https://example.com/feed",))

    class _Client:
        async def fetch_json(
            self, query: str, params: dict[str, Any],
        ) -> QueryResult:
            assert "FROM raw_posts" in query
            assert params == {"domain": source.domain,
                              "feed_urls": list(source.feed_urls),
                              "challenge_pattern": PAGE_CHALLENGE_PATTERN}

            return QueryResult([{
                "domain": source.domain, "post_id": "saved-id",
                "url": "https://example.com/story",
                "post_created_at": None, "scraped_at": now.isoformat(),
                "source": "rss", "request_url": source.feed_urls[0],
                "payload": json.dumps({
                    "feed_xml": "<item><title>Saved story</title></item>",
                    "page_html": "", "page_fetch_status": "failed",
                }),
            }])

    fetch = AsyncMock(return_value=FetchedText(
        "<article><p>Restored.</p></article>", "https://example.com/story",
    ))
    monkeypatch.setattr("dank.scrape.rss._fetch_text", fetch)
    batches = [
        batch async for batch in _discover_source_batches(
            _make_settings(), source, cast(Any, _Client()), cast(Any, None),
            cast(Any, None), feed_staleness=datetime.timedelta(days=14),
            batch_size=50, retry_failed_pages=True,
        )
    ]

    assert len(batches) == 1
    assert batches[0].posts[0].post_id == "saved-id"
    assert batches[0].posts[0].post_created_at is None
    fetch.assert_awaited_once()
    assert fetch.call_args.args[1] == "https://example.com/story"


@pytest.mark.parametrize("domain", ["x.com", "reddit.com"])
async def test_failed_page_mode_does_not_collect_browser_sources(
    domain: str,
) -> None:
    assert [
        batch async for batch in _discover_source_batches(
            _make_settings(), SourceConfig(domain, ()),
            cast(Any, None), cast(Any, None), cast(Any, None),
            feed_staleness=datetime.timedelta(days=14), batch_size=50,
            retry_failed_pages=True,
        )
    ] == []


async def test_articles_are_saved_before_waiting_for_media(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    saved: list[str] = []
    downloading = asyncio.Event()
    release = asyncio.Event()
    now = datetime.datetime.now(datetime.UTC)

    class _Client:
        async def insert_rows(self, table: str, rows: object) -> None:
            saved.append(table)

    async def download(*args: Any, **kwargs: Any) -> list[RawAsset]:
        downloading.set()
        await release.wait()

        return []

    monkeypatch.setattr("dank.scrape.runner.download_assets", download)
    queue: asyncio.Queue[ScrapeBatch | None] = asyncio.Queue()
    await queue.put(ScrapeBatch(
        [RawPost("test", "post", "https://test/post", now, now,
                 "rss", "https://test/feed", "{}")],
        [AssetDiscovery("rss", "test", "post", f"https://test/{i}",
                        "image", None) for i in range(2)],
    ))
    await queue.put(None)
    task = asyncio.create_task(_process_batches(
        queue, cast(Any, _Client()), cast(Any, None), assets_dir=tmp_path,
        browser_profile_dir=tmp_path, max_asset_bytes=None, batch_size=2,
    ))

    try:
        await asyncio.wait_for(downloading.wait(), timeout=1)
        assert saved == ["raw_posts"]
        assert not task.done()
    finally:
        release.set()
        await task


@pytest.mark.parametrize("domain,enabled,global_types,expected", [
    ("reddit.com", False, ("image", "audio", "video"), ("image", "audio")),
    ("reddit.com", True, ("image", "audio", "video"),
     ("image", "audio", "video")),
    ("reddit.com", True, ("image",), ("image",)),
    ("x.com", False, ("image", "video"), ("image", "video")),
])
async def test_reddit_video_opt_in_respects_global_download_policy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
    domain: str, global_types: tuple[str, ...], expected: tuple[str, ...],
    *, enabled: bool,
) -> None:
    source = SourceConfig(domain, ())
    settings = _make_settings()._replace(
        sources=(source,), media_download_types=global_types,
        reddit=RedditSettings(download_videos=enabled),
    )
    consumer = AsyncMock()

    async def discover(*_args: Any, **_kwargs: Any):
        yield ScrapeBatch([], [])

    monkeypatch.setattr(
        "dank.scrape.runner._discover_source_batches", discover,
    )
    monkeypatch.setattr("dank.scrape.runner._process_batches", consumer)
    history = RunMetrics(
        runtime_info(), source_count=1, batch_size=100,
        headless=True, options=ScrapeOptions(),
    )
    await _collect_source(
        settings, source, 1, AsyncMock(), AsyncMock(), AsyncMock(),
        history, tmp_path, tmp_path, asyncio.Lock(),
    )
    assert consumer.call_args.kwargs["download_types"] == expected
