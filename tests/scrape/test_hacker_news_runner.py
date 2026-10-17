from __future__ import annotations

import datetime as dt
import json
import pathlib
import sys
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import Settings, SourceConfig, load_settings
from dank.model import RawPost
from dank.scrape import (
    __main__ as cli,
    runner,
)
from dank.scrape.types import ScrapeBatch
from dank.scrape.zendriver import BrowserConfig

DOMAIN = "news.ycombinator.com"


class Client:
    def __init__(self) -> None:
        self.rows: dict[str, list[dict[str, Any]]] = {}

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


class Browser:
    def __init__(self, config: BrowserConfig) -> None:
        pass

    async def __aenter__(self) -> Browser:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


def setup(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> tuple[Settings, Client]:
    settings = load_settings("config.native.example.toml")._replace(
        data_dir=tmp_path,
        sources=(SourceConfig(DOMAIN, ()),),
    )
    client = Client()
    monkeypatch.setattr(runner, "ClickHouseClient", Mock(return_value=client))
    monkeypatch.setattr(runner, "BrowserSession", Browser)

    return settings, client


def post() -> RawPost:
    now = dt.datetime.now(dt.UTC)

    return RawPost(
        DOMAIN, "123", "https://news.ycombinator.com/item?id=123", now, now,
        "hacker_news", "https://hckrnews.com/",
        json.dumps({"hckrnews": {"id": 123, "link_text": "Story"}}),
    )


async def test_listing_persists_as_hacker_news_without_detail_fetch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    settings, client = setup(monkeypatch, tmp_path)

    async def collect(*args: object) -> AsyncIterator[ScrapeBatch]:
        yield ScrapeBatch([post()], [])

    details = Mock(side_effect=AssertionError("Unexpected detail fetch"))
    rss = Mock(side_effect=AssertionError("Unexpected RSS discovery"))
    monkeypatch.setattr(runner, "scrape_hacker_news", collect)
    monkeypatch.setattr(runner, "scrape_hacker_news_details", details)
    monkeypatch.setattr(runner, "_source_feed_urls", rss)
    await runner.run_scrape(settings, headless=True)
    assert client.rows["raw_posts"][0]["domain"] == DOMAIN
    assert client.rows["raw_posts"][0]["source"] == "hacker_news"
    assert client.rows["scrape_source_runs"][-1]["source"] == "hacker_news"
    assert not details.called
    assert not rss.called


async def test_details_select_only_saved_hacker_news_and_persist_each_batch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path,
) -> None:
    settings, client = setup(monkeypatch, tmp_path)
    settings = settings._replace(sources=(
        *settings.sources, SourceConfig("example.test", ()),
        SourceConfig(DOMAIN, (), tags=("duplicate",)),
    ))
    item = post()
    load = AsyncMock(return_value=[item])
    collected: list[RawPost] = []

    async def details(
        settings: object, posts: list[RawPost], http: object,
    ) -> AsyncIterator[ScrapeBatch]:
        collected.extend(posts)
        yield ScrapeBatch([item], [])
        assert len(client.rows["raw_posts"]) == 1

    listing = Mock(side_effect=AssertionError("Unexpected listing fetch"))
    monkeypatch.setattr(runner, "_load_pending_hacker_news_posts", load)
    monkeypatch.setattr(runner, "scrape_hacker_news_details", details)
    monkeypatch.setattr(runner, "scrape_hacker_news", listing)
    await runner.run_scrape(settings, headless=True, hacker_news_details=True)
    assert collected == [item]
    load.assert_awaited_once()
    assert {row["domain"] for row in client.rows["scrape_source_runs"]} == {
        DOMAIN,
    }
    assert not listing.called


@pytest.mark.parametrize("other", [
    "--reddit-details", "--x-details", "--retry-failed-pages",
    "--retry-youtube-thumbnails",
])
def test_detail_cli_rejects_conflicting_modes(
    monkeypatch: pytest.MonkeyPatch, other: str,
) -> None:
    run = Mock()
    monkeypatch.setattr(cli, "run_scrape_from_config", run)
    monkeypatch.setattr(
        sys, "argv", ["scrape", "--hacker-news-details", other],
    )

    with pytest.raises(SystemExit) as error:
        cli.main()

    assert error.value.code == 2
    assert not run.called


def test_detail_cli_dispatches_explicitly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = Mock()
    monkeypatch.setattr(cli, "run_scrape_from_config", run)
    monkeypatch.setattr(sys, "argv", ["scrape", "--hacker-news-details"])
    cli.main()
    assert run.call_args.kwargs["hacker_news_details"] is True
