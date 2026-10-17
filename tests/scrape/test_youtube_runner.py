from __future__ import annotations

import asyncio
import datetime as dt
import json
import pathlib
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import Settings, SourceConfig, load_settings
from dank.model import AssetDiscovery, RawAsset, RawPost
from dank.scrape import assets, runner
from dank.scrape.types import ScrapeBatch
from dank.scrape.youtube.payloads import discover_assets
from dank.scrape.zendriver import BrowserConfig


class Client:
    def __init__(self) -> None:
        self.rows: dict[str, list[dict[str, Any]]] = {}
        self.saved = {domain: asyncio.Event()
                      for domain in ("youtube.com", "reddit.com")}

    async def __aenter__(self) -> Client:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    async def execute(self, query: str) -> None:
        pass

    async def fetch_json(self, *args: object) -> SimpleNamespace:
        return SimpleNamespace(rows=[])

    async def insert_rows(
        self, table: str, rows: list[dict[str, Any]],
    ) -> None:
        self.rows.setdefault(table, []).extend(rows)

        if table == "raw_posts" and rows[0]["domain"] in self.saved:
            self.saved[rows[0]["domain"]].set()


class Browser:
    def __init__(self, config: BrowserConfig) -> None:
        self.config = config

    async def __aenter__(self) -> Browser:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


def post(*, shelf: str = "World news") -> RawPost:
    now = dt.datetime.now(dt.UTC)
    payload = {
        "schema_version": 1, "kind": "video", "video_id": "abcdefghijk",
        "title": "Example video", "channel_name": "Example channel",
        "is_live": False, "detail_capture": {"status": "disabled"},
        "discovery": {
            "feed_url": "https://www.youtube.com/feed/news_destination",
            "feed_name": "Top stories", "feed_kind": "news",
            "tab": "top_stories",
            "shelf": shelf, "rank": 1, "region": "GB", "playlist_id": "",
            "observed_at": now.isoformat(),
        },
        "renderer": {"videoId": "abcdefghijk"},
    }

    return RawPost(
        "youtube.com", "video:abcdefghijk",
        "https://www.youtube.com/watch?v=abcdefghijk", None, now, "youtube",
        "https://www.youtube.com/feed/news_destination", json.dumps(payload),
    )


def setup(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> tuple[Settings, Client]:
    settings = load_settings("config.native.example.toml")._replace(
        data_dir=tmp_path,
        sources=(SourceConfig("youtube.com", ()),),
    )
    client = Client()
    monkeypatch.setattr(runner, "ClickHouseClient", Mock(return_value=client))
    monkeypatch.setattr(runner, "BrowserSession", Browser)

    return settings, client


async def test_youtube_and_reddit_persist_while_x_is_waiting(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    settings, client = setup(monkeypatch, tmp_path)
    started, release = asyncio.Event(), asyncio.Event()
    used: dict[str, Browser] = {}

    async def discover(
        settings: Settings, source: SourceConfig, client: Any, http: Any,
        session: Browser, **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        used[source.domain] = session

        if source.domain == "x.com":
            started.set()
            await release.wait()
        else:
            await started.wait()

        item = post()

        if source.domain != "youtube.com":
            item = item._replace(domain=source.domain, source="rss")

        yield ScrapeBatch([item], [])

    monkeypatch.setattr(runner, "_discover_source_batches", discover)
    task = asyncio.create_task(runner.run_scrape(settings._replace(sources=(
        SourceConfig("x.com", ("account",)),
        SourceConfig("reddit.com", (), ("https://www.reddit.com/r/test/",)),
        SourceConfig("youtube.com", ()),
    )), headless=True))

    try:
        await asyncio.wait_for(asyncio.gather(
            *(event.wait() for event in client.saved.values()),
        ), timeout=2)
        assert not task.done()
        assert len({id(session) for session in used.values()}) == 3
    finally:
        release.set()
        await task

    assert client.rows["youtube_discoveries"][0]["shelf"] == "World news"
    source_rows = client.rows["scrape_source_runs"]
    assert any(row["source"] == "youtube" for row in source_rows)


@pytest.mark.parametrize("authenticated", [False, True])
async def test_profile_isolation_and_feed_appearances_survive_media_skip(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, *,
    authenticated: bool,
) -> None:
    settings, client = setup(monkeypatch, tmp_path)
    saved_profile = tmp_path / "saved-youtube"
    settings = settings._replace(youtube=settings.youtube._replace(
        authenticated=authenticated, profile_dir=saved_profile,
    ))
    profiles: list[pathlib.Path | None] = []

    async def discover(
        settings: Settings, source: SourceConfig, client: Any, http: Any,
        session: Browser, **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        profiles.append(session.config.profile_dir)
        first, second = post(), post(shelf="More on this story")
        yield ScrapeBatch([first, second], [AssetDiscovery(
            "youtube", "youtube.com", first.post_id, first.url,
            "youtube", None,
        )])

    monkeypatch.setattr(runner, "_discover_source_batches", discover)
    await runner.run_scrape(settings, headless=True)
    appearances = client.rows["youtube_discoveries"]
    assert {row["shelf"] for row in appearances} == {
        "World news", "More on this story",
    }
    assert len({row["run_id"] for row in appearances}) == 1
    assert client.rows["raw_assets"][0]["local_path"] == ""
    assert profiles[0] is not None

    if authenticated:
        assert profiles == [saved_profile]
    else:
        assert profiles[0] != saved_profile
        assert not profiles[0].exists()


async def test_enabled_video_collection_reaches_yt_dlp_not_page_downloader(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    settings = load_settings("config.native.example.toml")
    item = post()
    discoveries = discover_assets(item, settings.youtube._replace(
        scrape_videos=True,
    ))
    downloaded = RawAsset(
        item.domain, item.post_id, item.url, "youtube", item.scraped_at,
        "youtube", str(tmp_path / "video.mp4"),
    )
    video_downloader = AsyncMock(return_value=downloaded)
    page_downloader = AsyncMock(side_effect=AssertionError(
        "A YouTube watch page must not be downloaded as the video file",
    ))
    monkeypatch.setattr(assets, "download_audio_video_asset", video_downloader)
    monkeypatch.setattr(assets, "download_file_http", page_downloader)
    result = await assets.download_assets(
        discoveries, assets_dir=tmp_path,
        http_client=Mock(), download_types=("video",),
    )
    assert result == [downloaded]
    video_downloader.assert_awaited_once()
    page_downloader.assert_not_awaited()
