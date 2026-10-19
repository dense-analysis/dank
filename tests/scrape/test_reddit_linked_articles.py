from __future__ import annotations

import datetime as dt
import json
import sys
from collections.abc import AsyncIterator
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import RedditSettings, SourceConfig, load_settings
from dank.model import RawPost
from dank.scrape import (
    __main__ as cli,
    runner,
)
from dank.scrape.linked_articles import LinkedArticle
from dank.scrape.runner import (
    _discover_source_batches,  # pyright: ignore[reportPrivateUsage]
    _load_reddit_link_posts,  # pyright: ignore[reportPrivateUsage]
    _reddit_article_links,  # pyright: ignore[reportPrivateUsage]
    _reddit_content_batches,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.types import ScrapeBatch
from dank.storage.clickhouse import QueryResult

OBSERVED = dt.datetime(2026, 10, 18, 12, 30, tzinfo=dt.UTC)
SOURCE = SourceConfig("reddit.com", (), (
    "https://www.reddit.com/r/python/",
))


def post(
    url: str = "https://publisher.test/story", *,
    post_id: str = "t3_abc", **payload: object,
) -> RawPost:
    return RawPost(
        "reddit.com", post_id,
        f"https://www.reddit.com/r/python/comments/{post_id[3:]}/story/",
        OBSERVED - dt.timedelta(days=3), OBSERVED, "reddit",
        "https://www.reddit.com/r/python/top/?t=day",
        json.dumps({"title": "Story", "subreddit": "python",
                    "url": url, "is_self": False, **payload}),
    )


@pytest.mark.parametrize("url", [
    "https://www.reddit.com/comments/other", "https://m.reddit.com/r/python/",
    "https://v.redd.it/video", "https://i.redd.it/image.png",
    "https://preview.redd.it/image.jpg", "https://www.youtube.com/watch?v=123",
    "https://m.youtube.com/watch?v=123", "https://youtu.be/video",
    "https://x.com/user/status/123", "https://www.instagram.com/reel/video/",
    "https://vimeo.com/video", "https://m.tiktok.com/video",
    "https://news.ycombinator.com/item?id=123", "file:///private/file",
    "https://user:secret@publisher.test/story", "https://[broken/story",
])
def test_only_external_publisher_links_are_selected(url: str) -> None:
    assert _reddit_article_links([post(url)], set()) == {}


def test_self_posts_do_not_follow_links_from_their_body_or_destination(
) -> None:
    assert _reddit_article_links([post(
        is_self=True, selftext="https://publisher.test/story",
    )], set()) == {}


def test_destinations_deduplicate_and_preserve_source_observation() -> None:
    first = post()
    duplicate = post("https://publisher.test/story#part", post_id="t3_def")
    seen: set[str] = set()
    selected = _reddit_article_links([first, duplicate], seen)
    article, = selected.values()
    assert article.url == "https://publisher.test/story"
    assert article.origin == {
        "domain": "reddit.com", "post_id": first.post_id, "source": "reddit",
        "community": "python", "url": first.url,
        "observed_at": OBSERVED.isoformat(),
    }
    assert _reddit_article_links([duplicate], seen) == {}


@pytest.mark.parametrize("saved_domain, saved_html, expected_identity", [
    ("publisher.test", "<p>Saved body</p>", "reuse"),
    ("publisher.test", "", ("publisher.test", "saved-id")),
    ("lobste.rs", "<p>Aggregator excerpt</p>", None),
])
async def test_listing_precedes_publisher_and_readable_originals_are_reused(
    monkeypatch: pytest.MonkeyPatch, saved_domain: str, saved_html: str,
    expected_identity: object,
) -> None:
    settings = load_settings("config.native.example.toml")._replace(
        reddit=RedditSettings(collect_linked_articles=True),
    )
    item = post()
    lookup = AsyncMock(return_value={"https://publisher.test/story": {
        "domain": saved_domain, "post_id": "saved-id",
        "url": "https://publisher.test/story", "html": saved_html,
        "source": "rss",
    }})
    monkeypatch.setattr(runner, "_known_link_destinations", lookup)
    selected: list[LinkedArticle] = []

    async def listing() -> AsyncIterator[ScrapeBatch]:
        yield ScrapeBatch([item], [])
        yield ScrapeBatch([item], [])

    async def capture(
        http: object, articles: list[LinkedArticle], **kwargs: object,
    ) -> AsyncIterator[ScrapeBatch]:
        selected.extend(articles)

        if articles:
            yield ScrapeBatch([item._replace(
                domain="publisher.test", post_id="saved-id",
                source="linked_article", url="https://publisher.test/story",
            )], [])

    monkeypatch.setattr(runner, "scrape_linked_articles", capture)
    batches = _reddit_content_batches(
        settings, cast(Any, None), cast(Any, None), listing(), set(),
    )
    first = await anext(batches)
    assert first.posts == [item]
    lookup.assert_not_awaited()
    rest = [batch async for batch in batches]
    lookup.assert_awaited_once()
    assert [p.source for batch in rest for p in batch.posts] == (
        ["reddit"] if expected_identity == "reuse"
        else ["reddit", "linked_article"]
    )

    if expected_identity == "reuse":
        assert selected == []
    else:
        assert selected[0].identity == expected_identity
        assert selected[0].origin is not None
        assert selected[0].origin["observed_at"] == OBSERVED.isoformat()


async def test_disabled_collection_still_yields_reddit_posts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = load_settings("config.native.example.toml")
    lookup = AsyncMock()
    monkeypatch.setattr(runner, "_known_link_destinations", lookup)
    item = post()

    async def listing() -> AsyncIterator[ScrapeBatch]:
        yield ScrapeBatch([item], [])

    batches = [batch async for batch in _reddit_content_batches(
        settings, cast(Any, None), cast(Any, None), listing(), set(),
    )]
    assert batches == [ScrapeBatch([item], [])]
    lookup.assert_not_awaited()


@pytest.mark.parametrize("enabled", [False, True])
async def test_saved_backfill_never_navigates_reddit_or_resaves_discovery(
    monkeypatch: pytest.MonkeyPatch, *, enabled: bool,
) -> None:
    settings = load_settings("config.native.example.toml")._replace(
        reddit=RedditSettings(collect_linked_articles=enabled),
    )
    saved = AsyncMock(return_value=[post()])
    monkeypatch.setattr(runner, "_load_reddit_link_posts", saved)
    monkeypatch.setattr(runner, "_known_link_destinations", AsyncMock(
        return_value={},
    ))
    listing = Mock(side_effect=AssertionError("Must not navigate Reddit"))
    monkeypatch.setattr(runner, "scrape_reddit_communities", listing)
    selected: list[LinkedArticle] = []

    async def capture(
        http: object, articles: list[LinkedArticle], **kwargs: object,
    ) -> AsyncIterator[ScrapeBatch]:
        selected.extend(articles)
        yield ScrapeBatch([post()._replace(source="linked_article")], [])

    monkeypatch.setattr(runner, "scrape_linked_articles", capture)
    batches = [batch async for batch in _discover_source_batches(
        settings, SOURCE, cast(Any, None), cast(Any, None), cast(Any, None),
        feed_staleness=dt.timedelta(days=1), batch_size=50,
        reddit_linked_articles=True,
    )]
    assert bool(selected) is enabled
    assert [p.source for batch in batches for p in batch.posts] == (
        ["linked_article"] if enabled else []
    )
    assert saved.await_count == int(enabled)
    listing.assert_not_called()


async def test_saved_backfill_scope_limit_and_original_timestamp() -> None:
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([post()._asdict()])
    selected = await _load_reddit_link_posts(
        client, SOURCE, RedditSettings()._replace(
            scrape=RedditSettings().scrape._replace(max_posts=7),
        ),
    )
    assert selected == [post()]
    query, params = client.fetch_json.call_args.args
    assert params == {"communities": ["python"], "limit": 7}
    assert "ORDER BY scraped_at DESC LIMIT 1 BY post_id" in query
    assert "LIMIT %(limit)s BY community" in query
    assert "NOT JSONExtractBool(payload, 'is_self')" in query


def test_backfill_cli_dispatches_and_rejects_conflicting_modes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run = Mock()
    monkeypatch.setattr(cli, "run_scrape_from_config", run)
    monkeypatch.setattr(sys, "argv", ["scrape", "--reddit-linked-articles"])
    cli.main()
    assert run.call_args.kwargs["reddit_linked_articles"] is True
    run.reset_mock()
    monkeypatch.setattr(sys, "argv", [
        "scrape", "--reddit-linked-articles", "--reddit-details",
    ])

    with pytest.raises(SystemExit, match="2"):
        cli.main()

    run.assert_not_called()
