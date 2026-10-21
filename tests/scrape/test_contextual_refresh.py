from __future__ import annotations

import asyncio
import datetime as dt
import json
import pathlib
from collections.abc import AsyncIterator
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import Settings, SourceConfig, load_settings
from dank.model import RawPost
from dank.scrape import runner
from dank.scrape.metrics import count
from dank.scrape.types import ScrapeBatch
from dank.storage.clickhouse import QueryResult


class Client:
    def __init__(self, posts: list[RawPost] | None = None) -> None:
        self.posts = posts or []
        self.rows: dict[str, list[dict[str, Any]]] = {}
        self.queries: list[tuple[str, Any]] = []

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    async def execute(self, query: str) -> None:
        pass

    async def fetch_json(
        self, query: str, params: Any = None,
    ) -> QueryResult:
        self.queries.append((query, params))

        return QueryResult([post._asdict() for post in self.posts])

    async def insert_rows(
        self, table: str, rows: list[dict[str, Any]],
    ) -> None:
        self.rows.setdefault(table, []).extend(rows)


class Browser:
    def __init__(self, *args: object) -> None:
        pass

    async def __aenter__(self) -> Browser:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


def settings(tmp_path: pathlib.Path, domain: str = "reddit.com") -> Settings:
    return load_settings("config.native.example.toml")._replace(
        data_dir=tmp_path, sources=(SourceConfig(domain, ()),),
        media_download_types=(),
    )


def post(domain: str = "reddit.com") -> RawPost:
    now = dt.datetime.now(dt.UTC)

    return RawPost(
        domain, "saved", f"https://{domain}/saved", None, now,
        "reddit" if domain == "reddit.com" else "rss", "listing", "{}",
    )


@pytest.mark.parametrize(("domain", "collector"), [
    ("reddit.com", "scrape_reddit_details"),
    ("x.com", "scrape_x_posts"),
    ("news.ycombinator.com", "scrape_hacker_news_details"),
    ("example.test", "retry_feed_pages"),
])
async def test_post_refresh_only_uses_exact_saved_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
    domain: str, collector: str,
) -> None:
    saved = post(domain)
    client = Client([saved])
    calls: list[tuple[Any, ...]] = []

    async def collect(*args: Any, **kwargs: Any) -> AsyncIterator[ScrapeBatch]:
        calls.append(args)
        yield ScrapeBatch([saved], [])

    monkeypatch.setattr(runner, collector, collect)
    configured = settings(tmp_path, domain)
    batches = [batch async for batch in runner._discover_source_batches(  # pyright: ignore[reportPrivateUsage]
        configured, configured.sources[0], cast(Any, client), cast(Any, None),
        cast(Any, None), feed_staleness=dt.timedelta(days=14), batch_size=50,
        target_post=(domain, "saved"),
    )]

    assert batches == [ScrapeBatch([saved], [])]
    assert len(calls) == 1
    assert calls[0][1] == [saved]
    assert len(client.queries) == 1
    query, parameters = client.queries[0]
    assert "domain = %(domain)s AND post_id = %(post_id)s" in query
    assert parameters == {"domain": domain, "post_id": "saved"}

    if domain == "reddit.com":
        assert calls[0][0].capture_comments
    elif domain == "x.com":
        assert calls[0][0].conversation.enabled


async def test_unknown_post_cannot_trigger_collection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    collect = Mock()
    monkeypatch.setattr(runner, "scrape_reddit_details", collect)
    configured = settings(tmp_path)

    with pytest.raises(ValueError, match="must already be saved"):
        async for _ in runner._refresh_target_post(  # pyright: ignore[reportPrivateUsage]
            configured, configured.sources[0], cast(Any, Client()),
            cast(Any, None), cast(Any, None), ("reddit.com", "unknown"),
            batch_size=50,
        ):
            pass

    collect.assert_not_called()


@pytest.mark.parametrize(("failed", "empty", "status"), [
    (False, False, "completed"), (True, False, "partial"),
    (True, True, "failed"),
])
async def test_scoped_run_returns_persisted_keys_and_real_partial_outcome(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, *, failed: bool,
    empty: bool, status: str,
) -> None:
    client = Client()
    monkeypatch.setattr(runner, "ClickHouseClient", Mock(return_value=client))
    monkeypatch.setattr(runner, "BrowserSession", Browser)
    linked = AsyncMock()
    transcripts = AsyncMock()
    monkeypatch.setattr(runner, "collect_linked_youtube_metadata", linked)
    monkeypatch.setattr(runner, "collect_run_youtube_transcripts", transcripts)

    async def discover(
        *args: Any, **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        if failed:
            count("fetch_failures")

        if not empty:
            yield ScrapeBatch([post()], [])

    monkeypatch.setattr(runner, "_discover_source_batches", discover)
    result = await runner.run_scrape(settings(tmp_path), scoped=True)

    assert result.status == status
    assert result.collected_posts == (
        set() if empty else {("reddit.com", "saved")}
    )
    assert result.counts()["posts_saved"] == (0 if empty else 1)
    assert client.rows["scrape_runs"][-1]["status"] == result.status
    linked.assert_not_awaited()
    transcripts.assert_awaited_once()


async def test_collection_lock_excludes_overlap_and_releases_after_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    started, release = asyncio.Event(), asyncio.Event()

    async def collect(*args: Any, **kwargs: Any) -> Any:
        started.set()
        await release.wait()
        raise RuntimeError("provider stopped")

    monkeypatch.setattr(runner, "_run_scrape", collect)
    configured = settings(tmp_path)
    active = asyncio.create_task(runner.run_scrape(configured))
    await started.wait()

    try:
        with pytest.raises(runner.CollectionBusyError):
            await runner.run_scrape(configured)
    finally:
        release.set()

        with pytest.raises(RuntimeError, match="provider stopped"):
            await active

    with pytest.raises(RuntimeError, match="provider stopped"):
        await runner.run_scrape(configured)


async def test_single_video_refresh_has_no_feed_or_backlog_collection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    video = post("youtube.com")._replace(post_id="video:abcdefghijk")
    client = Client([video])
    metadata = AsyncMock(return_value={
        "status": "captured", "request_url": "https://www.youtube.com/oembed",
        "payload": json.dumps({"title": "New video title"}),
    })
    transcript = AsyncMock(return_value={"status": "captured"})
    monkeypatch.setattr(runner, "collect_youtube_video_metadata", metadata)
    monkeypatch.setattr(runner, "collect_youtube_video_transcript", transcript)
    configured = settings(tmp_path, "youtube.com")
    batches = [batch async for batch in runner._refresh_target_post(  # pyright: ignore[reportPrivateUsage]
        configured, configured.sources[0], cast(Any, client), cast(Any, None),
        cast(Any, None), ("youtube.com", video.post_id), batch_size=50,
    )]

    assert len(batches) == 1
    assert batches[0].posts[0].post_id == video.post_id
    payload = json.loads(batches[0].posts[0].payload)
    assert payload["title"] == "New video title"
    assert metadata.call_args.args[2] == "abcdefghijk"
    assert transcript.call_args.args[2] == "abcdefghijk"
    assert len(client.queries) == 1


@pytest.mark.parametrize(("domain", "collector", "capture_fields"), [
    ("reddit.com", "scrape_reddit_details",
     ("detail_capture", "comment_capture")),
    ("x.com", "scrape_x_posts", ("conversation_capture",)),
    ("news.ycombinator.com", "scrape_hacker_news_details",
     ("comment_capture",)),
])
@pytest.mark.parametrize(("capture_status", "reported_failure", "expected"), [
    ("captured", False, "completed"),
    ("failed", False, "partial"),
    ("partial", False, "partial"),
    ("failed", True, "partial"),
])
async def test_target_payload_outcomes_determine_refresh_status(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
    domain: str, collector: str, capture_fields: tuple[str, ...],
    capture_status: str, *, reported_failure: bool, expected: str,
) -> None:
    client = Client()
    saved = post(domain)
    monkeypatch.setattr(runner, "ClickHouseClient", Mock(return_value=client))
    monkeypatch.setattr(runner, "BrowserSession", Browser)
    monkeypatch.setattr(runner, "_load_target_post", AsyncMock(
        return_value=saved,
    ))

    async def collect(
        *args: Any, **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        if reported_failure:
            count("fetch_failures")

        payload = {field: {"status": capture_status}
                   for field in capture_fields}
        yield ScrapeBatch([saved._replace(payload=json.dumps(payload))], [])

    monkeypatch.setattr(runner, collector, collect)
    metrics = await runner.run_scrape(
        settings(tmp_path, domain), target_post=(domain, saved.post_id),
    )

    assert metrics.status == expected
    assert metrics.sources[1].status == expected
    assert metrics.counts()["fetch_failures"] == (
        0 if capture_status == "captured" else 1
    )
    assert metrics.collected_posts == {(domain, saved.post_id)}
