from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from dank.config import (
    Settings,
    SourceAdapter,
    SourceAdapterConfig,
    SourceConfig,
    load_settings,
)
from dank.scrape.youtube.feeds import configured_feeds
from dank.storage.clickhouse import QueryResult
from dank.web.reader_refresh_scope import resolve_scope

YOUTUBE = "https://www.youtube.com"
VIDEO = "L_6pAVHYoFM"
CHANNEL = "UC1234567890123456789012"


def settings(*sources: SourceConfig) -> Settings:
    return load_settings("config.native.example.toml")._replace(
        sources=sources,
    )


def client(rows: list[dict[str, Any]] | None = None) -> Any:
    result = AsyncMock()
    result.fetch_json.return_value = QueryResult(rows or [])

    return result


def feed_urls(result: Settings) -> list[str]:
    return [feed.url for source in result.sources
            for feed in configured_feeds(result.youtube, source)]


async def test_all_preserves_all_configured_collection_adapters():
    original = settings(
        SourceConfig("one.test", ()), SourceConfig("x.com", ("alice",)),
        SourceConfig("news.ycombinator.com", (), adapters=(
            SourceAdapterConfig(SourceAdapter.HACKER_NEWS),
            SourceAdapterConfig(SourceAdapter.RSS, ("https://feed.test/rss",)),
        )),
    )
    storage = client()
    plan = await resolve_scope(original, storage, {"kind": "all"})
    assert [source.domain for source in plan.settings.sources] == [
        "one.test", "x.com", "news.ycombinator.com",
    ]
    assert plan.settings.sources[-1].collection_adapters == (
        original.sources[-1].collection_adapters
    )
    assert plan.target_post is None
    storage.fetch_json.assert_not_called()


async def test_hacker_news_refresh_preserves_compound_and_rss_only_sources():
    original = settings(
        SourceConfig("news.ycombinator.com", (), adapters=(
            SourceAdapterConfig(SourceAdapter.HACKER_NEWS),
            SourceAdapterConfig(SourceAdapter.RSS, ("https://a.test/rss",)),
        )),
        SourceConfig("news.ycombinator.com", (), adapters=(
            SourceAdapterConfig(SourceAdapter.RSS, ("https://b.test/rss",)),
        )),
        SourceConfig("publisher.test", ()),
    )
    plan = await resolve_scope(original, client(), {"kind": "hacker_news"})
    assert plan.settings.sources == original.sources[:2]
    assert [adapter.kind for source in plan.settings.sources
            for adapter in source.collection_adapters] == [
        "hacker_news", "rss", "rss",
    ]


async def test_blogs_refresh_excludes_platform_rss_and_keeps_publisher_feeds():
    original = settings(
        SourceConfig("news.ycombinator.com", (), adapters=(
            SourceAdapterConfig(SourceAdapter.RSS, ("https://a.test/rss",)),
        )),
        SourceConfig("publisher.test", (), adapters=(
            SourceAdapterConfig(SourceAdapter.RSS, ("https://b.test/rss",)),
            SourceAdapterConfig(SourceAdapter.RSS, ("https://c.test/rss",)),
        )),
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Python/",)),
        SourceConfig("youtube.com", ()),
        SourceConfig("x.com", ("alice",)),
    )
    plan = await resolve_scope(original, client(), {"kind": "rss"})
    assert plan.settings.sources == (original.sources[1],)
    assert [adapter.feed_urls for adapter in
            plan.settings.sources[0].collection_adapters] == [
        ("https://b.test/rss",), ("https://c.test/rss",),
    ]


async def test_domains_and_tags_are_union_within_and_intersection_across():
    original = settings(
        SourceConfig("a.test", (), tags=("tech",)),
        SourceConfig("b.test", (), tags=("games",)),
        SourceConfig("c.test", (), tags=("tech",)),
        SourceConfig("b.test", (), tags=("unrelated",)),
    )
    plan = await resolve_scope(original, client(), {
        "kind": "all", "domains": ["b.test", "a.test", "a.test"],
        "tags": ["TECH", "games"],
    })
    assert plan.settings.sources == original.sources[:2]
    assert plan.scope == {
        "kind": "all", "domains": ["a.test", "b.test"],
        "tags": ["games", "tech"],
    }


async def test_reddit_community_narrows_every_duplicate_source_entry():
    original = settings(
        SourceConfig("reddit.com", (), feed_urls=(
            "https://www.reddit.com/r/Python/", "https://www.reddit.com/r/Games/",
        )),
        SourceConfig("reddit.com", (), feed_urls=(
            "https://www.reddit.com/r/python/",
        )),
        SourceConfig("x.com", ("python",)),
    )
    plan = await resolve_scope(original, client(), {
        "kind": "reddit", "community": "r/PYTHON",
    })
    assert len(plan.settings.sources) == 2
    assert all(len(source.feed_urls) == 1 for source in plan.settings.sources)
    assert all("python" in source.feed_urls[0].lower()
               for source in plan.settings.sources)
    assert all(source.collection_adapters[0].feed_urls == source.feed_urls
               for source in plan.settings.sources)
    assert plan.label == "r/python"


async def test_x_account_narrows_configured_accounts_without_prefix_matches():
    original = settings(
        SourceConfig("x.com", ("Alice", "alice_news", "@Bob")),
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Alice/",)),
    )
    plan = await resolve_scope(original, client(), {
        "kind": "x", "account": "@ALICE",
    })
    assert len(plan.settings.sources) == 1
    assert plan.settings.sources[0].accounts == ("Alice",)
    assert plan.scope["account"] == "alice"


async def test_youtube_channel_uses_saved_identity_without_default_feeds():
    original = settings(SourceConfig("youtube.com", ("Alice", "Bob")))
    original = original._replace(youtube=original.youtube._replace(
        authenticated=True, homepage=True,
        hype_playlists=(("US", YOUTUBE + "/playlist?list=usa"),),
    ))
    storage = client([{
        "feed_url": YOUTUBE + "/@Alice/videos", "channel_id": CHANNEL,
        "channel_url": YOUTUBE + "/@Alice",
    }])
    plan = await resolve_scope(original, storage, {
        "kind": "youtube", "channel_id": CHANNEL,
    })
    assert feed_urls(plan.settings) == [YOUTUBE + "/@Alice/videos"]
    assert not plan.settings.youtube.hype
    assert not plan.settings.youtube.news_tabs
    assert not plan.settings.youtube.homepage
    assert plan.settings.youtube.authenticated
    assert "youtube_discoveries" in storage.fetch_json.call_args.args[0]
    assert original.sources[0].accounts == ("Alice", "Bob")


async def test_youtube_channel_url_works_before_first_capture():
    original = settings(SourceConfig("youtube.com", ("Alice", "Bob")))
    plan = await resolve_scope(original, client(), {
        "kind": "youtube", "channel_id": YOUTUBE + "/@Alice",
    })
    assert feed_urls(plan.settings) == [YOUTUBE + "/@Alice/videos"]


async def test_youtube_channel_refresh_keeps_its_configured_tabs_only():
    original = settings(SourceConfig("youtube.com", (), feed_urls=(
        YOUTUBE + "/@Alice/videos", YOUTUBE + "/@Alice/posts",
        YOUTUBE + "/@Bob/shorts",
    )))
    plan = await resolve_scope(original, client(), {
        "kind": "youtube", "channel_id": YOUTUBE + "/@Alice",
    })
    assert feed_urls(plan.settings) == [
        YOUTUBE + "/@Alice/videos", YOUTUBE + "/@Alice/posts",
    ]


@pytest.mark.parametrize("target", [
    YOUTUBE + "/feed/news_destination/world", YOUTUBE + "/feed/hype",
])
async def test_single_youtube_feed_avoids_other_defaults(target: str):
    original = settings(SourceConfig("youtube.com", ("Alice",)))
    plan = await resolve_scope(original, client(), {
        "kind": "youtube", "feed_id": target,
    })
    assert feed_urls(plan.settings) == [target]


async def test_youtube_home_keeps_session_requirement_without_other_feeds():
    original = settings(SourceConfig("youtube.com", ("Alice",)))
    original = original._replace(youtube=original.youtube._replace(
        authenticated=True, homepage=True,
    ))
    plan = await resolve_scope(original, client(), {
        "kind": "youtube", "feed_id": YOUTUBE + "/",
    })
    assert feed_urls(plan.settings) == [YOUTUBE + "/"]
    assert plan.settings.youtube.authenticated


async def test_regional_hype_preserves_region_without_other_playlists():
    target = YOUTUBE + "/playlist?list=usa"
    original = settings(SourceConfig("youtube.com", ("Alice",)))
    original = original._replace(youtube=original.youtube._replace(
        hype_playlists=(("GB", YOUTUBE + "/playlist?list=gb"), ("US", target)),
    ))
    plan = await resolve_scope(original, client(), {
        "kind": "youtube", "feed_id": target,
    })
    assert feed_urls(plan.settings) == [target]
    selected = configured_feeds(
        plan.settings.youtube, plan.settings.sources[0],
    )
    assert selected[0].kind == "hype"
    assert selected[0].region == "US"


@pytest.mark.parametrize("scope", [
    {"kind": "reddit", "community": "unknown"},
    {"kind": "x", "account": "unknown"},
    {"kind": "youtube", "channel_id": "unknown"},
    {"kind": "youtube", "feed_id": "https://unrelated.test/"},
    {"kind": "all", "domains": ["unknown.test"]},
    {"kind": "all", "tags": ["unknown"]},
    {"kind": "rss", "domains": ["reddit.com"]},
    {"kind": "rss", "tags": ["social"]},
])
async def test_unknown_or_empty_context_never_falls_back_to_all(scope: object):
    original = settings(
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Python/",),
                     tags=("social",)),
        SourceConfig("x.com", ("Alice",)),
        SourceConfig("youtube.com", ("Alice",)),
        SourceConfig("publisher.test", ()),
    )

    with pytest.raises(ValueError):
        await resolve_scope(original, client(), scope)


async def test_saved_unconfigured_channel_is_not_authorized_by_its_capture():
    original = settings(SourceConfig("youtube.com", ("Alice",)))
    storage = client([{
        "feed_url": YOUTUBE + "/@Removed/videos", "channel_id": CHANNEL,
        "channel_url": YOUTUBE + "/@Removed",
    }])

    with pytest.raises(ValueError, match="not configured"):
        await resolve_scope(original, storage, {
            "kind": "youtube", "channel_id": CHANNEL,
        })


async def test_defaults_do_not_authorize_youtube_without_configured_source():
    with pytest.raises(ValueError, match="No configured sources"):
        await resolve_scope(settings(), client(), {
            "kind": "youtube", "feed_id": YOUTUBE + "/feed/hype",
        })


@pytest.mark.parametrize("scope", [
    None, [], {}, {"kind": "unsupported"}, {"kind": True},
    {"kind": "all", "domains": "publisher.test"},
    {"kind": "all", "tags": [False]},
    {"kind": "all", "domains": [""]},
    {"kind": "all", "community": "python"},
    {"kind": "reddit", "community": "https://reddit.com/r/python"},
    {"kind": "x", "account": "alice/status/1"},
    {"kind": "youtube", "channel_id": "alice", "feed_id": "feed"},
    {"kind": "post", "domain": "publisher.test"},
    {"kind": "post", "domain": "publisher.test", "post_id": "id", "url": "x"},
    {"kind": "post", "domain": "youtube.com", "post_id": "post:community"},
    {"kind": "post", "domain": "youtube.com", "post_id": "video:bad"},
])
async def test_invalid_scopes_fail_before_database_access(scope: object):
    storage = client()

    with pytest.raises(ValueError):
        await resolve_scope(settings(), storage, scope)

    storage.fetch_json.assert_not_called()


async def test_known_linked_article_is_refreshable_without_publisher_config():
    storage = client([{"domain": "linked.test", "post_id": "article"}])
    plan = await resolve_scope(settings(), storage, {
        "kind": "post", "domain": "linked.test", "post_id": "article",
    })
    assert plan.target_post == ("linked.test", "article")
    assert plan.settings.sources == (SourceConfig("linked.test", ()),)
    query, params = storage.fetch_json.call_args.args
    assert "raw_posts" in query
    assert "is_reference" not in query
    assert params == {"domain": "linked.test", "post_id": "article"}


async def test_known_linked_video_is_refreshable_without_source_config():
    storage = client([{"video_id": VIDEO}])
    plan = await resolve_scope(settings(), storage, {
        "kind": "post", "domain": "youtube.com", "post_id": "video:" + VIDEO,
    })
    assert plan.target_post == ("youtube.com", "video:" + VIDEO)
    assert plan.settings.sources == (SourceConfig("youtube.com", ()),)
    query, params = storage.fetch_json.call_args.args
    assert "youtube_reference_evidence" in query
    assert "youtube_discoveries" in query
    assert "raw_posts" in query
    assert params["id"] == VIDEO


@pytest.mark.parametrize("domain,post_id", [
    ("publisher.test", "https://publisher.test/arbitrary"),
    ("youtube.com", "video:" + VIDEO),
])
async def test_unknown_post_never_becomes_arbitrary_collection(
    domain: str, post_id: str,
):
    with pytest.raises(ValueError, match="no saved collection target"):
        await resolve_scope(settings(), client(), {
            "kind": "post", "domain": domain, "post_id": post_id,
        })


async def test_known_post_preserves_one_matching_configured_source():
    original = settings(
        SourceConfig("publisher.test", ()),
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Python/",)),
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Games/",)),
    )
    plan = await resolve_scope(original, client([{"post_id": "saved"}]), {
        "kind": "post", "domain": "reddit.com", "post_id": "saved",
    })
    assert plan.settings.sources == (original.sources[1],)
    assert plan.target_post == ("reddit.com", "saved")
