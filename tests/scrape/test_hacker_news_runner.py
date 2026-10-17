from __future__ import annotations

import datetime as dt
import json
import pathlib
import sys
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import (
    HackerNewsSettings,
    Settings,
    SourceConfig,
    load_settings,
)
from dank.model import RawPost
from dank.scrape import (
    __main__ as cli,
    runner,
)
from dank.scrape.runner import (
    _load_pending_hacker_news_posts,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.types import ScrapeBatch
from dank.scrape.zendriver import BrowserConfig
from dank.storage.clickhouse import ClickHouseClient

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


async def test_pending_details_restore_comments_without_reverting_latest_story(
) -> None:
    latest = post()
    latest_payload = json.loads(latest.payload) | {
        'story': {'id': 123, 'title': 'Updated story', 'time': 123456},
        'article': {'url': 'https://example.test/current'},
    }
    comments = [{'id': 124, 'type': 'comment', 'parent': 123, 'text': 'Saved'}]
    capture = {'status': 'partial', 'captured': 1, 'reason': 'comment_limit'}
    history = {
        'story': {'id': 123, 'title': 'Old story'},
        'comments': comments, 'comment_capture': capture,
    }
    row = latest._asdict() | {
        'payload': json.dumps(latest_payload),
        'detail_payload': json.dumps(history),
    }
    client = SimpleNamespace(fetch_json=AsyncMock(
        return_value=SimpleNamespace(rows=[row]),
    ))
    posts = await _load_pending_hacker_news_posts(
        cast(ClickHouseClient, client), HackerNewsSettings(),
    )
    payload = json.loads(posts[0].payload)
    assert payload['comments'] == comments
    assert payload['comment_capture'] == capture
    assert payload['story'] == latest_payload['story']
    assert payload['article'] == latest_payload['article']
    assert posts[0].post_created_at == latest.post_created_at
    assert posts[0].scraped_at == latest.scraped_at


@pytest.mark.parametrize('comment_limit', [None, 25, 50, 1000])
async def test_pending_details_query_prioritizes_history_and_respects_budget(
    comment_limit: int | None,
) -> None:
    client = SimpleNamespace(fetch_json=AsyncMock(
        return_value=SimpleNamespace(rows=[]),
    ))
    settings = HackerNewsSettings(
        comment_limit=comment_limit, detail_max_posts=7,
    )
    assert await _load_pending_hacker_news_posts(
        cast(ClickHouseClient, client), settings,
    ) == []
    query, params = client.fetch_json.call_args.args
    assert params['all_comments'] is (comment_limit is None)
    assert params['comment_limit'] == (comment_limit or 0)
    assert params['limit'] == 7
    assert "argMaxIf(payload, scraped_at" in query
    assert "history.complete = 0" in query
    assert "'captured') < %(comment_limit)s" in query
    assert "'status') = 'partial' DESC" in query
    assert query.endswith('LIMIT %(limit)s')


async def test_pending_details_preserve_never_detailed_listing() -> None:
    latest = post()
    client = SimpleNamespace(fetch_json=AsyncMock(
        return_value=SimpleNamespace(rows=[
            latest._asdict() | {'detail_payload': ''},
        ]),
    ))
    selected = await _load_pending_hacker_news_posts(
        cast(ClickHouseClient, client), HackerNewsSettings(),
    )
    assert selected == [latest]


@pytest.mark.parametrize("saved_domain", ["lobste.rs", "publisher.test"])
async def test_article_reuses_original_but_not_an_aggregators_identity(
    monkeypatch: pytest.MonkeyPatch, tmp_path: pathlib.Path, saved_domain: str,
) -> None:
    settings, client = setup(monkeypatch, tmp_path)
    url = "https://publisher.test/story"
    item = post()._replace(payload=json.dumps({"hckrnews": {
        "link": url, "link_text": "Story",
    }}))

    async def collect(*args: object) -> AsyncIterator[ScrapeBatch]:
        yield ScrapeBatch([item], [])

    monkeypatch.setattr(runner, "scrape_hacker_news", collect)
    monkeypatch.setattr(runner, "_known_link_destinations", AsyncMock(
        return_value={url: {"domain": saved_domain, "post_id": "saved",
                           "url": url, "html": "<p>Saved article</p>",
                           "source": "rss"}},
    ))
    requested: list[Any] = []

    async def articles(
        http: object, selected: list[Any], **kwargs: Any,
    ) -> AsyncIterator[ScrapeBatch]:
        requested.extend(selected)

        if selected:
            yield ScrapeBatch([item._replace(
                domain="publisher.test", post_id="canonical", url=url,
                source="linked_article",
            )], [])

    monkeypatch.setattr(runner, "scrape_linked_articles", articles)
    await runner.run_scrape(settings, headless=True)
    assert bool(requested) == (saved_domain == "lobste.rs")

    if requested:
        assert requested[0].identity is None
        assert client.rows["raw_posts"][-1]["domain"] == "publisher.test"


async def test_saved_destination_lookup_uses_stable_result_column_names(
) -> None:
    from dank.scrape.runner import (
        _known_link_destinations,  # pyright: ignore[reportPrivateUsage]
    )

    url = "https://publisher.test/story"
    client = AsyncMock()
    client.fetch_json.return_value = SimpleNamespace(rows=[{
        "lookup_url": url, "domain": "publisher.test", "post_id": "saved",
        "url": url, "html": "<p>Saved body</p>", "source": "rss",
    }])
    result = await _known_link_destinations(client, [url])
    query = client.fetch_json.call_args.args[0]

    for name in ("domain", "post_id", "url", "html", "source"):
        assert f"p.{name} AS {name}" in query

    assert result[url]["domain"] == "publisher.test"
    client.fetch_json.assert_awaited_once()
