from __future__ import annotations

import asyncio
import datetime
import pathlib
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any, NamedTuple, cast
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import ScrapeSettings, Settings, SourceConfig, load_settings
from dank.model import AssetDiscovery, RawAsset, RawPost
from dank.runtime import RuntimeInfo
from dank.scrape.assets import download_assets
from dank.scrape.assets.http import http_asset_path
from dank.scrape.metrics import SourceMetrics, count, current_source
from dank.scrape.runner import run_scrape
from dank.scrape.types import ScrapeBatch


class _Client:
    def __init__(self) -> None:
        self.rows: dict[str, list[dict[str, Any]]] = {}
        self.statements: list[str] = []
        self.fail_posts = False
        self.fail_history = False
        self.last_write: dict[str, datetime.datetime] = {}

    async def __aenter__(self) -> _Client:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    async def execute(self, statement: str) -> None:
        self.statements.append(statement)

    async def insert_rows(
        self, table: str, rows: list[dict[str, Any]],
    ) -> None:
        if table == "raw_posts" and self.fail_posts:
            raise RuntimeError("post insert failed")

        if table == "scrape_runs" and rows[0]["version"] == 2:
            if self.fail_history:
                raise ValueError("history insert failed")

        self.rows.setdefault(table, []).extend(dict(row) for row in rows)

        if table == "raw_posts":
            for row in rows:
                self.last_write[row["domain"]] = datetime.datetime.now(
                    datetime.UTC,
                )


class _Browser:
    async def __aenter__(self) -> _Browser:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


class _Harness(NamedTuple):
    settings: Settings
    client: _Client


@pytest.fixture
def harness(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> _Harness:
    path = tmp_path / "settings.fixture.toml"
    path.write_text('sources = ["one.test", "two.test"]\n')
    settings = load_settings(path)._replace(data_dir=tmp_path / "data")
    client = _Client()
    monkeypatch.setattr(
        "dank.scrape.runner.ClickHouseClient", Mock(return_value=client),
    )
    monkeypatch.setattr(
        "dank.scrape.runner.BrowserSession", Mock(return_value=_Browser()),
    )
    monkeypatch.setattr("dank.scrape.runner.runtime_info", lambda: RuntimeInfo(
        "container", "Linux aarch64", "3.13.4", 0.5, 1024 ** 3, "test-code",
    ))

    return _Harness(settings, client)


def _post(domain: str) -> RawPost:
    now = datetime.datetime.now(datetime.UTC)

    return RawPost(
        domain, "post", f"https://{domain}/article", now, now,
        "rss", f"https://{domain}/feed", "{}",
    )


async def test_history_tracks_source_completion_after_persistence(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def discover(
        settings: Settings, source: SourceConfig, *args: Any, **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        count("http_requests", 2)

        if source.domain == "two.test":
            count("fetch_failures")

        yield ScrapeBatch([_post(source.domain)], [])

    monkeypatch.setattr(
        "dank.scrape.runner._discover_source_batches", discover,
    )
    await run_scrape(harness.settings)
    runs = harness.client.rows["scrape_runs"]
    sources = harness.client.rows["scrape_source_runs"]
    final = runs[-1]
    assert len(harness.client.statements) == 6
    assert runs[0]["status"] == "running"
    assert runs[0]["finished_at"] is None
    assert final["version"] == 2
    assert final["status"] == "partial"
    assert final["posts_saved"] == 2
    assert final["http_requests"] == 4
    assert final["cpu_limit"] == 0.5
    assert final["memory_limit_bytes"] == 1024 ** 3
    assert final["source_concurrency"] == 2
    assert final["http_concurrency"] == 16
    assert final["http_per_host"] == 2
    assert final["media_concurrency"] == 4
    assert final["queue_batches"] == 2
    assert final["media_download_types"] == ["image", "audio", "video"]
    assert final["code_version"] == "test-code"
    completed = [row for row in sources if row["version"] == 2]
    assert [row["status"] for row in completed] == ["completed", "partial"]
    assert {row["run_id"] for row in sources} == {final["run_id"]}
    assert [row["posts_saved"] for row in completed] == [1, 1]
    assert all(row["elapsed_ms"] >= row["collection_ms"] for row in completed)
    assert all(
        row["finished_at"] >= harness.client.last_write[row["domain"]]
        for row in completed
    )
    assert current_source.get() is None

    await run_scrape(harness.settings)
    final_runs = [row for row in runs if row["version"] == 2]
    assert len({row["run_id"] for row in final_runs}) == 2


async def test_database_failure_preserves_original_error_and_unsaved_counts(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def discover(
        *args: Any, **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        yield ScrapeBatch([_post("one.test")], [])

    monkeypatch.setattr(
        "dank.scrape.runner._discover_source_batches", discover,
    )
    harness.client.fail_posts = True

    with pytest.raises(RuntimeError, match="post insert failed"):
        await run_scrape(harness.settings._replace(
            sources=(SourceConfig("one.test", ()),),
        ))

    final = harness.client.rows["scrape_runs"][-1]
    assert final["status"] == "failed"
    assert final["error_type"] == "RuntimeError"
    assert final["posts_saved"] == 0
    assert harness.client.rows["scrape_source_runs"][-1]["status"] == "failed"
    harness.client.fail_history = True

    with pytest.raises(RuntimeError, match="post insert failed"):
        await run_scrape(harness.settings)


async def test_cancelled_run_records_cancellation_without_leaking_tasks(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch,
) -> None:
    waiting = asyncio.Event()

    async def discover(
        *args: Any, **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        yield ScrapeBatch([_post("one.test")], [])
        waiting.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(
        "dank.scrape.runner._discover_source_batches", discover,
    )
    task = asyncio.create_task(run_scrape(harness.settings))
    await waiting.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert harness.client.rows["scrape_runs"][-1]["status"] == "cancelled"
    final = harness.client.rows["scrape_source_runs"][-1]
    assert final["status"] == "cancelled"
    assert final["posts_saved"] == 0
    assert final["finished_at"] is not None


@pytest.mark.parametrize("failed", [False, True])
async def test_no_feed_source_is_skipped_or_failed(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, *, failed: bool,
) -> None:
    async def discover(
        *args: Any, **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        source = current_source.get()
        assert source is not None
        source.skipped = True

        if failed:
            count("fetch_failures")

        for _ in ():
            yield ScrapeBatch([], [])

    monkeypatch.setattr(
        "dank.scrape.runner._discover_source_batches", discover,
    )
    await run_scrape(harness.settings)
    expected = "failed" if failed else "skipped"
    assert harness.client.rows["scrape_runs"][-1]["status"] == expected
    assert harness.client.rows["scrape_source_runs"][-1]["status"] == expected


async def test_file_measurements_distinguish_cached_new_failed_and_skipped(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    stats = SourceMetrics(1, SourceConfig("one.test", ()))
    assets = [
        AssetDiscovery("rss", "one.test", "post", url, kind, None)
        for url, kind in (
            ("https://one.test/cached.jpg", "image"),
            ("https://one.test/new.jpg", "image"),
            ("https://one.test/fail.jpg", "image"),
            ("https://one.test/embed", "iframe"),
        )
    ]
    directory = tmp_path / "one.test/post"
    directory.mkdir(parents=True)
    http_asset_path(directory, assets[0].url).write_bytes(b"cached")

    async def download(
        *, discovery: AssetDiscovery, timestamp: datetime.datetime,
        **kwargs: Any,
    ) -> RawAsset:
        path = http_asset_path(directory, discovery.url)
        local_path = ""

        if not discovery.url.endswith("fail.jpg"):
            if not path.exists():
                path.write_bytes(b"new file")

            local_path = str(path)

        return RawAsset(
            discovery.domain, discovery.post_id, discovery.url, "image",
            timestamp, "rss", local_path,
        )

    monkeypatch.setattr(
        "dank.scrape.assets.download_file_http", download,
    )
    await download_assets(
        assets, assets_dir=tmp_path, http_client=cast(Any, None),
        owners=dict.fromkeys(assets, stats),
    )
    assert stats.counts["files_cached"] == 1
    assert stats.counts["files_downloaded"] == 1
    assert stats.counts["downloaded_file_bytes"] == len(b"new file")
    assert stats.counts["files_failed"] == 1
    assert stats.counts["media_skipped"] == 1
    assert stats.active_media == 0


def test_history_schema_matches_fresh_database_schema() -> None:
    root = pathlib.Path(__file__).resolve().parents[2]
    schema = (root / "src/dank/scrape/history.sql").read_text()
    qualified = schema.replace(
        "IF NOT EXISTS scrape_", "IF NOT EXISTS dank.scrape_",
    ).replace("ALTER TABLE scrape_", "ALTER TABLE dank.scrape_")
    assert qualified in (root / "schema.sql").read_text()
    assert "history.sql" in (root / "pyproject.toml").read_text()


async def test_http_counts_are_isolated_between_concurrent_sources() -> None:
    from dank.scrape.metrics import request_metrics, retry_sleep

    sources = [SourceMetrics(i, SourceConfig(f"{i}.test", ())) for i in (1, 2)]

    async def work(source: SourceMetrics) -> None:
        token = current_source.set(source)

        try:
            with request_metrics(retry=source.index == 2):
                await asyncio.sleep(0)
                count("http_429", source.index)

            await retry_sleep(0)
        finally:
            current_source.reset(token)

    await asyncio.gather(*(work(source) for source in sources))
    assert [source.counts["http_requests"] for source in sources] == [1, 1]
    assert [source.counts["retries"] for source in sources] == [0, 1]
    assert [source.counts["http_429"] for source in sources] == [1, 2]
    assert all(source.active_http == source.waiting_retries == 0
               for source in sources)


@pytest.mark.parametrize("accounts", [(), ("example",)])
async def test_x_missing_accounts_and_login_failure_have_correct_outcomes(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch,
    accounts: tuple[str, ...],
) -> None:
    from dank.scrape.x import LoginRequiredError, scrape_x_accounts

    async def blocked(*args: Any, **kwargs: Any) -> AsyncIterator[ScrapeBatch]:
        raise LoginRequiredError("login unavailable")
        yield ScrapeBatch([], [])

    monkeypatch.setattr("dank.scrape.x._scrape_account", blocked)
    browser = SimpleNamespace(wait=AsyncMock(), main_tab=object())
    session = SimpleNamespace(get_browser=AsyncMock(return_value=browser))
    stats = SourceMetrics(1, SourceConfig("x.com", accounts))
    token = current_source.set(stats)

    try:
        batches = [
            batch async for batch in scrape_x_accounts(
                harness.settings.x, accounts, None, cast(Any, session),
            )
        ]
    finally:
        current_source.reset(token)

    stats.collected()
    assert batches == []
    assert stats.status == ("failed" if accounts else "skipped")
    assert stats.counts["fetch_failures"] == int(bool(accounts))
    assert stats.error_type == ("LoginRequiredError" if accounts else "")


async def test_history_snapshots_source_options_and_limit_between_runs(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def discover(
        *args: Any, **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        yield ScrapeBatch([_post("one.test")], [])

    monkeypatch.setattr(
        "dank.scrape.runner._discover_source_batches", discover,
    )
    source = SourceConfig(
        "one.test", (), ("https://one.test/feed",), ("ddd",),
    )
    await run_scrape(harness.settings._replace(
        sources=(source,), max_entries_per_feed=20,
        media_download_types=("image",),
    ))
    await run_scrape(harness.settings._replace(
        sources=(source._replace(tags=("architecture",)),),
        max_entries_per_feed=5, media_download_types=(),
    ))
    sources = [
        row for row in harness.client.rows["scrape_source_runs"]
        if row["version"] == 2
    ]
    runs = [
        row for row in harness.client.rows["scrape_runs"]
        if row["version"] == 2
    ]
    assert [row["tags"] for row in sources] == [["ddd"], ["architecture"]]
    assert all(
        row["feed_urls"] == ["https://one.test/feed"] for row in sources
    )
    assert [row["max_entries_per_feed"] for row in runs] == [20, 5]
    assert [row["media_download_types"] for row in runs] == [["image"], []]


async def test_fast_source_is_saved_while_another_source_waits(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch,
) -> None:
    slow_started = asyncio.Event()
    release_slow = asyncio.Event()
    fast_saved = asyncio.Event()
    insert = harness.client.insert_rows

    async def insert_rows(table: str, rows: list[dict[str, Any]]) -> None:
        await insert(table, rows)

        if table == "scrape_source_runs" and any(
            row["domain"] == "two.test" and row["status"] == "completed"
            for row in rows
        ):
            fast_saved.set()

    async def discover(
        settings: Settings, source: SourceConfig, *args: Any, **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        if source.domain == "one.test":
            slow_started.set()
            await release_slow.wait()
        else:
            await slow_started.wait()

        yield ScrapeBatch([_post(source.domain)], [])

    monkeypatch.setattr(harness.client, "insert_rows", insert_rows)
    monkeypatch.setattr(
        "dank.scrape.runner._discover_source_batches", discover,
    )
    task = asyncio.create_task(run_scrape(harness.settings))

    try:
        await asyncio.wait_for(fast_saved.wait(), timeout=2)
        assert not task.done()
        assert [row["domain"] for row in harness.client.rows["raw_posts"]] == [
            "two.test",
        ]
    finally:
        release_slow.set()
        await task


@pytest.mark.parametrize("limit", [1, 3])
async def test_source_concurrency_is_bounded_and_x_is_serialized(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, limit: int,
) -> None:
    active = peak = browsers = browser_peak = 0

    async def discover(
        settings: Settings, source: SourceConfig, *args: Any, **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        nonlocal active, peak, browsers, browser_peak
        active += 1
        browsers += int(source.domain == "x.com")
        peak = max(peak, active)
        browser_peak = max(browser_peak, browsers)
        await asyncio.sleep(0.01)
        yield ScrapeBatch([_post(source.domain)], [])
        active -= 1
        browsers -= int(source.domain == "x.com")

    monkeypatch.setattr(
        "dank.scrape.runner._discover_source_batches", discover,
    )
    settings = harness.settings._replace(
        sources=tuple(SourceConfig(domain, ()) for domain in (
            "x.com", "a.test", "x.com", "b.test", "c.test", "d.test",
        )),
        scrape=ScrapeSettings(source_concurrency=limit),
    )
    await run_scrape(settings)
    assert peak == limit
    assert browser_peak == 1
    final = harness.client.rows["scrape_runs"][-1]
    assert final["posts_saved"] == 6
    assert final["sources_started"] == 6


@pytest.mark.parametrize("cancel", [False, True])
async def test_full_queue_stops_producer_and_cleans_up_on_failure(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch, *, cancel: bool,
) -> None:
    produced = 0
    writing = asyncio.Event()
    release_writer = asyncio.Event()
    closed = asyncio.Event()
    insert = harness.client.insert_rows
    tasks_before = asyncio.all_tasks()

    async def insert_rows(table: str, rows: list[dict[str, Any]]) -> None:
        if table == "raw_posts":
            writing.set()
            await release_writer.wait()
            raise RuntimeError("writer failed")

        await insert(table, rows)

    async def discover(
        *args: Any, **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        nonlocal produced

        try:
            for _ in range(1000):
                produced += 1
                yield ScrapeBatch([_post("one.test")], [])
        finally:
            closed.set()

    monkeypatch.setattr(harness.client, "insert_rows", insert_rows)
    monkeypatch.setattr(
        "dank.scrape.runner._discover_source_batches", discover,
    )
    settings = harness.settings._replace(
        sources=(SourceConfig("one.test", ()),),
        scrape=ScrapeSettings(queue_batches=2),
    )
    task = asyncio.create_task(run_scrape(settings, batch_size=1))
    await asyncio.wait_for(writing.wait(), timeout=2)
    await asyncio.sleep(0.01)
    # One in the writer, two queued, one blocked in queue.put.
    assert produced == 4

    if cancel:
        task.cancel()
    else:
        release_writer.set()

    with pytest.raises(asyncio.CancelledError if cancel else RuntimeError):
        await asyncio.wait_for(task, timeout=2)

    await asyncio.wait_for(closed.wait(), timeout=2)
    assert asyncio.all_tasks() == tasks_before
    final = harness.client.rows["scrape_runs"][-1]
    assert final["posts_saved"] == 0
    assert final["status"] == ("cancelled" if cancel else "failed")


async def test_disabled_media_is_saved_as_references_without_failing_run(
    harness: _Harness, monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("INFO")

    async def discover(
        settings: Settings, source: SourceConfig, *args: Any, **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        yield ScrapeBatch([_post(source.domain)], [
            AssetDiscovery("rss", source.domain, "post", f"https://one/{kind}",
                           kind, None)
            for kind in ("image", "audio", "video", "youtube")
        ])

    monkeypatch.setattr(
        "dank.scrape.runner._discover_source_batches", discover,
    )
    http = AsyncMock(side_effect=AssertionError("HTTP must not be used"))
    youtube = AsyncMock(side_effect=AssertionError("yt-dlp must not be used"))
    monkeypatch.setattr("dank.scrape.assets.download_file_http", http)
    monkeypatch.setattr(
        "dank.scrape.assets.download_audio_video_asset", youtube,
    )
    await run_scrape(harness.settings._replace(media_download_types=()))
    http.assert_not_called()
    youtube.assert_not_called()
    final = harness.client.rows["scrape_runs"][-1]
    assert final["status"] == "completed"
    assert final["media_download_types"] == []
    assert final["media_skipped"] == 8
    assert final["files_failed"] == final["files_downloaded"] == 0
    assert final["asset_records_saved"] == 8
    assert final["posts_saved"] == 2
    assert len(harness.client.rows["raw_assets"]) == 8
    assert all(row["local_path"] == "" for row in
               harness.client.rows["raw_assets"])
    assert "Media downloads: disabled" in caplog.text
    assert "media skipped=8" in caplog.text
