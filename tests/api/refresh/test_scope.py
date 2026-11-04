"""Which sources a refresh scope selects, given saved targets as values."""

from __future__ import annotations

import pathlib
from typing import Any

import pytest

from dank.api.refresh.model import ChannelFeed
from dank.api.refresh.service import resolve_scope
from dank.config import (
    Settings,
    SourceAdapter,
    SourceAdapterConfig,
    SourceConfig,
    load_settings,
)
from dank.errors import InvalidInput
from dank.scrape.youtube.feeds import configured_feeds
from tests.api.refresh.fakes import FakeRefreshTargets, UnreachableTargets

EXAMPLE = pathlib.Path(__file__).parents[3] / "config.native.example.toml"
YOUTUBE = "https://www.youtube.com"
VIDEO = "L_6pAVHYoFM"
CHANNEL = "UC1234567890123456789012"


def configured(*sources: SourceConfig) -> Settings:
    return load_settings(EXAMPLE)._replace(sources=sources)


def targets(
    *,
    channels: tuple[ChannelFeed, ...] = (),
    playlists: dict[str, str] | None = None,
    posts: tuple[tuple[str, str], ...] = (),
    videos: tuple[str, ...] = (),
) -> FakeRefreshTargets:
    fake = FakeRefreshTargets()
    fake.channels = list(channels)
    fake.playlists = playlists or {}
    fake.posts = set(posts)
    fake.videos = set(videos)

    return fake


def feed_urls(result: Settings) -> list[str]:
    return [feed.url for source in result.sources
            for feed in configured_feeds(result.youtube, source)]


async def test_all_preserves_all_configured_collection_adapters() -> None:
    original = configured(
        SourceConfig("one.test", ()), SourceConfig("x.com", ("alice",)),
        SourceConfig("news.ycombinator.com", (), adapters=(
            SourceAdapterConfig(SourceAdapter.HACKER_NEWS),
            SourceAdapterConfig(SourceAdapter.RSS, ("https://feed.test/rss",)),
        )),
    )
    plan = await resolve_scope(original, UnreachableTargets(), {"kind": "all"})

    assert [source.domain for source in plan.settings.sources] == [
        "one.test", "x.com", "news.ycombinator.com",
    ]
    assert plan.settings.sources[-1].collection_adapters == (
        original.sources[-1].collection_adapters
    )
    assert plan.target_post is None


async def test_hacker_news_refresh_preserves_compound_and_rss_only_sources(
) -> None:
    original = configured(
        SourceConfig("news.ycombinator.com", (), adapters=(
            SourceAdapterConfig(SourceAdapter.HACKER_NEWS),
            SourceAdapterConfig(SourceAdapter.RSS, ("https://a.test/rss",)),
        )),
        SourceConfig("news.ycombinator.com", (), adapters=(
            SourceAdapterConfig(SourceAdapter.RSS, ("https://b.test/rss",)),
        )),
        SourceConfig("publisher.test", ()),
    )
    plan = await resolve_scope(original, targets(), {"kind": "hacker_news"})

    assert plan.settings.sources == original.sources[:2]
    assert [adapter.kind for source in plan.settings.sources
            for adapter in source.collection_adapters] == [
        "hacker_news", "rss", "rss",
    ]


async def test_blog_source_feeds_refresh_includes_hacker_news_and_publishers(
) -> None:
    original = configured(
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
    plan = await resolve_scope(original, targets(), {
        "kind": "rss", "blog_feed": "sources",
    })

    assert plan.settings.sources == original.sources[:2]
    assert [adapter.feed_urls for adapter in
            plan.settings.sources[1].collection_adapters] == [
        ("https://b.test/rss",), ("https://c.test/rss",),
    ]


async def test_domains_and_tags_are_union_within_and_intersection_across(
) -> None:
    original = configured(
        SourceConfig("a.test", (), tags=("tech",)),
        SourceConfig("b.test", (), tags=("games",)),
        SourceConfig("c.test", (), tags=("tech",)),
        SourceConfig("b.test", (), tags=("unrelated",)),
    )
    plan = await resolve_scope(original, targets(), {
        "kind": "all", "domains": ["b.test", "a.test", "a.test"],
        "tags": ["TECH", "games"],
    })

    assert plan.settings.sources == (
        original.sources[0], original.sources[1], original.sources[3],
    )
    assert plan.scope == {
        "kind": "all", "domains": ["a.test", "b.test"],
        "tags": ["games", "tech"],
    }


async def test_reddit_community_narrows_every_duplicate_source_entry() -> None:
    original = configured(
        SourceConfig("reddit.com", (), feed_urls=(
            "https://www.reddit.com/r/Python/",
            "https://www.reddit.com/r/Games/",
        )),
        SourceConfig("reddit.com", (), feed_urls=(
            "https://www.reddit.com/r/python/",
        )),
        SourceConfig("x.com", ("python",)),
    )
    plan = await resolve_scope(original, targets(), {
        "kind": "reddit", "community": "r/PYTHON",
    })

    assert len(plan.settings.sources) == 2
    assert all(len(source.feed_urls) == 1 for source in plan.settings.sources)
    assert all("python" in source.feed_urls[0].lower()
               for source in plan.settings.sources)
    assert all(source.collection_adapters[0].feed_urls == source.feed_urls
               for source in plan.settings.sources)
    assert plan.label == "r/python"


async def test_x_account_narrows_accounts_without_prefix_matches(
) -> None:
    original = configured(
        SourceConfig("x.com", ("Alice", "alice_news", "@Bob")),
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Alice/",)),
    )
    plan = await resolve_scope(original, targets(), {
        "kind": "x", "account": "@ALICE",
    })

    assert len(plan.settings.sources) == 1
    assert plan.settings.sources[0].accounts == ("Alice",)
    assert plan.scope["account"] == "alice"


async def test_home_and_account_refresh_remain_separate() -> None:
    original = configured(SourceConfig("x.com", ("alice", "bob")))
    home = await resolve_scope(
        original, targets(), {"kind": "x", "feed_id": "for_you"},
    )

    assert home.settings.x.homepage
    assert home.settings.sources[0].accounts == ()
    assert home.label == "X For You"

    account = await resolve_scope(
        original, targets(), {"kind": "x", "account": "alice"},
    )

    assert not account.settings.x.homepage
    assert account.settings.sources[0].accounts == ("alice",)

    with pytest.raises(InvalidInput):
        await resolve_scope(
            original,
            targets(),
            {"kind": "x", "feed_id": "for_you", "account": "alice"},
        )


async def test_youtube_channel_uses_saved_identity_without_default_feeds(
) -> None:
    original = configured(SourceConfig("youtube.com", ("Alice", "Bob")))
    original = original._replace(youtube=original.youtube._replace(
        authenticated=True, homepage=True,
        hype_playlists=(("US", YOUTUBE + "/playlist?list=usa"),),
    ))
    plan = await resolve_scope(
        original,
        targets(channels=(ChannelFeed(
            YOUTUBE + "/@Alice/videos", CHANNEL, YOUTUBE + "/@Alice",
        ),)),
        {"kind": "youtube", "channel_id": CHANNEL},
    )

    assert feed_urls(plan.settings) == [YOUTUBE + "/@Alice/videos"]
    assert not plan.settings.youtube.hype
    assert not plan.settings.youtube.news_tabs
    assert not plan.settings.youtube.homepage
    assert plan.settings.youtube.authenticated
    assert original.sources[0].accounts == ("Alice", "Bob")


async def test_youtube_channel_url_works_before_first_capture() -> None:
    original = configured(SourceConfig("youtube.com", ("Alice", "Bob")))
    plan = await resolve_scope(original, targets(), {
        "kind": "youtube", "channel_id": YOUTUBE + "/@Alice",
    })

    assert feed_urls(plan.settings) == [YOUTUBE + "/@Alice/videos"]


async def test_youtube_channel_refresh_keeps_its_tabs_only() -> None:
    original = configured(SourceConfig("youtube.com", (), feed_urls=(
        YOUTUBE + "/@Alice/videos", YOUTUBE + "/@Alice/posts",
        YOUTUBE + "/@Bob/shorts",
    )))
    plan = await resolve_scope(original, targets(), {
        "kind": "youtube", "channel_id": YOUTUBE + "/@Alice",
    })

    assert feed_urls(plan.settings) == [
        YOUTUBE + "/@Alice/videos", YOUTUBE + "/@Alice/posts",
    ]


@pytest.mark.parametrize("target", [
    YOUTUBE + "/feed/news_destination/world", YOUTUBE + "/feed/hype",
])
async def test_single_youtube_feed_avoids_other_defaults(target: str) -> None:
    original = configured(SourceConfig("youtube.com", ("Alice",)))
    plan = await resolve_scope(original, targets(), {
        "kind": "youtube", "feed_id": target,
    })

    assert feed_urls(plan.settings) == [target]


async def test_youtube_home_keeps_session_requirement_without_other_feeds(
) -> None:
    original = configured(
        SourceConfig("youtube.com", ("Alice",)),
        SourceConfig("reddit.com", (), ("https://reddit.com/r/python/",)),
    )
    original = original._replace(youtube=original.youtube._replace(
        authenticated=True, homepage=True,
        hype_playlists=(("US", YOUTUBE + "/playlist?list=usa"),),
    ))
    plan = await resolve_scope(original, UnreachableTargets(), {
        "kind": "youtube", "feed_id": YOUTUBE + "/",
    })

    assert feed_urls(plan.settings) == [YOUTUBE + "/"]
    assert len(plan.settings.sources) == 1
    assert plan.settings.sources[0].accounts == ()
    assert plan.settings.youtube.authenticated
    assert plan.settings.youtube.homepage
    assert not plan.settings.youtube.hype
    assert not plan.settings.youtube.hype_playlists
    assert not plan.settings.youtube.news_tabs
    assert plan.label == "Recommendations"


@pytest.mark.parametrize(("authenticated", "homepage"), [
    (False, False), (False, True), (True, False),
])
async def test_recommendations_refresh_requires_enabled_authenticated_home(
    *, authenticated: bool, homepage: bool,
) -> None:
    original = configured(SourceConfig("youtube.com", ("Alice",)))
    original = original._replace(youtube=original.youtube._replace(
        authenticated=authenticated, homepage=homepage,
    ))

    with pytest.raises(InvalidInput):
        await resolve_scope(original, targets(), {
            "kind": "youtube", "feed_id": YOUTUBE + "/",
        })


async def test_regional_hype_keeps_region_without_other_playlists() -> None:
    target = YOUTUBE + "/playlist?list=usa"
    original = configured(SourceConfig("youtube.com", ("Alice",)))
    original = original._replace(youtube=original.youtube._replace(
        hype_playlists=(("GB", YOUTUBE + "/playlist?list=gb"), ("US", target)),
    ))
    plan = await resolve_scope(original, targets(), {
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
    {"kind": "rss", "blog_feed": "sources", "tags": ["social"]},
    {"kind": "all", "domains": ["publisher.test"], "tags": ["social"]},
])
async def test_unknown_or_empty_context_never_falls_back_to_all(
    scope: object,
) -> None:
    original = configured(
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Python/",),
                     tags=("social",)),
        SourceConfig("x.com", ("Alice",)),
        SourceConfig("youtube.com", ("Alice",)),
        SourceConfig("publisher.test", ()),
    )

    with pytest.raises(InvalidInput):
        await resolve_scope(original, targets(), scope)


async def test_a_channel_that_is_not_a_url_says_why() -> None:
    original = configured(SourceConfig("youtube.com", ("Alice",)))

    with pytest.raises(InvalidInput, match="Invalid IPv6 URL"):
        await resolve_scope(original, targets(), {
            "kind": "youtube", "channel_id": "http://[",
        })


async def test_home_feed_without_a_session_says_what_it_needs() -> None:
    original = configured(SourceConfig("youtube.com", (), feed_urls=(
        YOUTUBE + "/",
    )))

    with pytest.raises(InvalidInput, match="YouTube home requires"):
        await resolve_scope(original, targets(), {
            "kind": "youtube", "feed_id": YOUTUBE + "/",
        })


async def test_saved_unconfigured_channel_is_not_authorized_by_its_capture(
) -> None:
    original = configured(SourceConfig("youtube.com", ("Alice",)))
    saved = targets(channels=(ChannelFeed(
        YOUTUBE + "/@Removed/videos", CHANNEL, YOUTUBE + "/@Removed",
    ),))

    with pytest.raises(InvalidInput, match="not configured"):
        await resolve_scope(original, saved, {
            "kind": "youtube", "channel_id": CHANNEL,
        })


async def test_defaults_do_not_authorize_youtube_without_configured_source(
) -> None:
    with pytest.raises(InvalidInput, match="No configured sources"):
        await resolve_scope(configured(), targets(), {
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
async def test_invalid_scopes_fail_before_reading_anything_saved(
    scope: object,
) -> None:
    with pytest.raises(InvalidInput):
        await resolve_scope(configured(), UnreachableTargets(), scope)


async def test_known_linked_article_needs_no_publisher_config(
) -> None:
    plan = await resolve_scope(
        configured(),
        targets(posts=(("linked.test", "article"),)),
        {"kind": "post", "domain": "linked.test", "post_id": "article"},
    )

    assert plan.target_post == ("linked.test", "article")
    assert plan.settings.sources == (SourceConfig("linked.test", ()),)
    assert plan.label == "Post"


async def test_known_linked_video_needs_no_source_config() -> None:
    plan = await resolve_scope(
        configured(),
        targets(videos=(VIDEO,)),
        {"kind": "post", "domain": "youtube.com", "post_id": "video:" + VIDEO},
    )

    assert plan.target_post == ("youtube.com", "video:" + VIDEO)
    assert plan.settings.sources == (SourceConfig("youtube.com", ()),)
    assert plan.label == "YouTube video"


@pytest.mark.parametrize(("domain", "post_id"), [
    ("publisher.test", "https://publisher.test/arbitrary"),
    ("youtube.com", "video:" + VIDEO),
])
async def test_unknown_post_never_becomes_arbitrary_collection(
    domain: str, post_id: str,
) -> None:
    with pytest.raises(InvalidInput, match="no saved collection target"):
        await resolve_scope(configured(), targets(), {
            "kind": "post", "domain": domain, "post_id": post_id,
        })


async def test_known_post_preserves_one_matching_configured_source() -> None:
    original = configured(
        SourceConfig("publisher.test", ()),
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Python/",)),
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Games/",)),
    )
    plan = await resolve_scope(
        original,
        targets(posts=(("reddit.com", "saved"),)),
        {"kind": "post", "domain": "reddit.com", "post_id": "saved"},
    )

    assert plan.settings.sources == (original.sources[1],)
    assert plan.target_post == ("reddit.com", "saved")


async def test_tagged_domain_keeps_other_communities_visible() -> None:
    original = configured(
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Python/",),
                     tags=("tech",)),
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Games/",),
                     tags=("games",)),
        SourceConfig("publisher.test", (), tags=("games",)),
    )
    broad = await resolve_scope(original, targets(), {
        "kind": "reddit", "tags": ["tech"],
    })

    assert broad.settings.sources == original.sources[:2]

    narrow = await resolve_scope(original, targets(), {
        "kind": "reddit", "tags": ["tech"], "community": "Games",
    })

    assert len(narrow.settings.sources) == 1
    assert narrow.settings.sources[0].feed_urls == (
        "https://reddit.com/r/Games/",
    )


async def test_tagged_domain_still_limits_x_to_selected_account() -> None:
    original = configured(
        SourceConfig("x.com", ("Alice",), tags=("tech",)),
        SourceConfig("x.com", ("Bob",), tags=("games",)),
    )
    plan = await resolve_scope(original, targets(), {
        "kind": "x", "tags": ["tech"], "account": "Bob",
    })

    assert plan.settings.sources == (original.sources[1],)


@pytest.mark.parametrize(("platforms", "domains", "label"), [
    (["reddit"], ["reddit.com"], "Reddit"),
    (["youtube", "reddit", "reddit"], ["reddit.com", "youtube.com"],
     "Reddit + YouTube"),
    (["rss"],
     ["publisher.test", "reddit.com", "news.ycombinator.com", "x.com"],
     "Blogs"),
    (["hacker_news"], ["news.ycombinator.com"], "Hacker News"),
])
async def test_all_platform_filters_follow_reader_domains(
    platforms: list[str], domains: list[str], label: str,
) -> None:
    original = configured(
        SourceConfig("publisher.test", ()),
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Python/",)),
        SourceConfig("youtube.com", ()),
        SourceConfig("news.ycombinator.com", (), adapters=(
            SourceAdapterConfig(SourceAdapter.HACKER_NEWS),
            SourceAdapterConfig(SourceAdapter.RSS, ("https://hn.test/rss",)),
        )),
        SourceConfig("x.com", ("Alice",)),
    )
    plan = await resolve_scope(original, targets(), {
        "kind": "all", "platforms": platforms,
    })

    assert [source.domain for source in plan.settings.sources] == domains
    assert plan.scope["platforms"] == sorted(set(platforms))
    assert plan.label == label

    if platforms == ["hacker_news"]:
        assert plan.settings.sources[0].collection_adapters == (
            original.sources[3].collection_adapters
        )


async def test_platforms_intersect_domain_and_tag_membership() -> None:
    original = configured(
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Python/",),
                     tags=("tech",)),
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Games/",),
                     tags=("games",)),
        SourceConfig("youtube.com", (), tags=("tech",)),
        SourceConfig("x.com", ("Alice",), tags=("tech",)),
    )
    plan = await resolve_scope(original, targets(), {
        "kind": "all", "platforms": ["reddit", "youtube"],
        "domains": ["reddit.com", "x.com"], "tags": ["tech"],
    })

    assert plan.settings.sources == original.sources[:2]
    assert plan.label == "Reddit + YouTube · selected sources"


async def test_empty_platform_filter_is_omitted_and_does_not_narrow() -> None:
    original = configured(SourceConfig("publisher.test", ()))
    plan = await resolve_scope(original, targets(), {
        "kind": "all", "platforms": [],
    })

    assert plan.scope == {"kind": "all"}
    assert plan.settings.sources == original.sources
    assert plan.label == "All sources"


@pytest.mark.parametrize("scope", [
    {"kind": "all", "platforms": "reddit"},
    {"kind": "all", "platforms": [True]},
    {"kind": "all", "platforms": ["stories"]},
    {"kind": "all", "platforms": ["all"]},
    {"kind": "all", "platforms": ["reddit", "unknown"]},
    {"kind": "rss", "platforms": []},
    {"kind": "reddit", "platforms": ["reddit"]},
    {"kind": "post", "domain": "publisher.test", "post_id": "saved",
     "platforms": ["rss"]},
])
async def test_invalid_platform_filter_fails_before_reading_anything_saved(
    scope: object,
) -> None:
    with pytest.raises(InvalidInput):
        await resolve_scope(configured(), UnreachableTargets(), scope)


@pytest.mark.parametrize("filters", [
    {"platforms": ["youtube"]},
    {"platforms": ["reddit"], "domains": ["publisher.test"]},
    {"platforms": ["reddit"], "tags": ["publisher-tag"]},
])
async def test_empty_platform_intersection_never_broadens_scope(
    filters: dict[str, Any],
) -> None:
    original = configured(
        SourceConfig("publisher.test", (), tags=("publisher-tag",)),
        SourceConfig("reddit.com", (), ("https://reddit.com/r/Python/",)),
    )

    with pytest.raises(InvalidInput, match="No configured sources"):
        await resolve_scope(original, targets(), {"kind": "all", **filters})


async def test_playlist_refresh_targets_saved_playlist_without_default_feeds(
) -> None:
    original = configured(SourceConfig("youtube.com", ("Alice",)))
    original = original._replace(youtube=original.youtube._replace(
        authenticated=True, homepage=True,
    ))
    plan = await resolve_scope(
        original,
        targets(playlists={"PLsaved": "Saved mix"}),
        {"kind": "youtube", "playlist_id": "PLsaved"},
    )

    assert feed_urls(plan.settings) == [YOUTUBE + "/playlist?list=PLsaved"]
    assert not plan.settings.youtube.playlists
    assert not plan.settings.youtube.homepage
    assert not plan.settings.youtube.hype
    assert not plan.settings.youtube.news_tabs
    assert plan.settings.youtube.authenticated
    assert plan.label == "Saved mix"


async def test_saved_playlist_without_a_title_gets_a_generic_label() -> None:
    original = configured(SourceConfig("youtube.com", ("Alice",)))
    plan = await resolve_scope(
        original,
        targets(playlists={"PLsaved": ""}),
        {"kind": "youtube", "playlist_id": "PLsaved"},
    )

    assert plan.label == "Playlist"


async def test_saved_playlist_with_a_malformed_id_is_no_target() -> None:
    original = configured(SourceConfig("youtube.com", ("Alice",)))

    with pytest.raises(InvalidInput, match="No saved or configured"):
        await resolve_scope(
            original,
            targets(playlists={"not a playlist": "Title"}),
            {"kind": "youtube", "feed_id": "playlists"},
        )


async def test_playlist_refresh_accepts_configured_playlist_before_capture(
) -> None:
    original = configured(SourceConfig("youtube.com", (), feed_urls=(
        YOUTUBE + "/playlist?list=PLconfigured",
    )))
    plan = await resolve_scope(original, targets(), {
        "kind": "youtube", "playlist_id": "PLconfigured",
    })

    assert feed_urls(plan.settings) == [
        YOUTUBE + "/playlist?list=PLconfigured",
    ]
    assert not plan.settings.youtube.playlists


async def test_playlist_library_leaves_saved_targets_to_bounded_collection(
) -> None:
    original = configured(SourceConfig("youtube.com", ("Alice",), feed_urls=(
        YOUTUBE + "/playlist?list=PLconfigured",
    )))
    original = original._replace(youtube=original.youtube._replace(
        authenticated=True, homepage=True,
    ))
    plan = await resolve_scope(
        original,
        targets(playlists={"PLsaved": "Saved mix"}),
        {"kind": "youtube", "feed_id": "playlists"},
    )

    assert feed_urls(plan.settings) == [
        YOUTUBE + "/playlist?list=PLconfigured",
    ]
    assert plan.settings.youtube.playlists
    assert plan.settings.youtube.authenticated
    assert not plan.settings.youtube.homepage
    assert not plan.settings.youtube.hype
    assert not plan.settings.youtube.news_tabs
    assert plan.label == "Playlists"


async def test_authenticated_empty_playlist_library_can_discover_saved_lists(
) -> None:
    original = configured(SourceConfig("youtube.com", ("Alice",)))
    original = original._replace(youtube=original.youtube._replace(
        authenticated=True,
    ))
    plan = await resolve_scope(original, targets(), {
        "kind": "youtube", "feed_id": "playlists",
    })

    assert feed_urls(plan.settings) == []
    assert plan.settings.youtube.playlists


@pytest.mark.parametrize("scope", [
    {"kind": "youtube", "playlist_id": "PLunknown"},
    {"kind": "youtube", "feed_id": "playlists"},
])
async def test_unknown_playlist_targets_never_fall_back_to_home(
    scope: object,
) -> None:
    original = configured(SourceConfig("youtube.com", ("Alice",)))

    with pytest.raises(InvalidInput):
        await resolve_scope(original, targets(), scope)


@pytest.mark.parametrize("scope", [
    {"kind": "youtube", "playlist_id": "PLx", "channel_id": "UCx"},
    {"kind": "youtube", "playlist_id": "PLx", "feed_id": "playlists"},
    {"kind": "youtube", "playlist_id": "https://example.com/list"},
])
async def test_invalid_playlist_scopes_fail_before_reading_anything_saved(
    scope: object,
) -> None:
    with pytest.raises(InvalidInput):
        await resolve_scope(configured(), UnreachableTargets(), scope)


async def test_narrow_youtube_feed_never_also_collects_library() -> None:
    original = configured(SourceConfig("youtube.com", ("Alice",)))
    original = original._replace(youtube=original.youtube._replace(
        authenticated=True, homepage=True, playlists=True,
    ))
    plan = await resolve_scope(original, targets(), {
        "kind": "youtube", "feed_id": YOUTUBE + "/",
    })

    assert not plan.settings.youtube.playlists
    assert feed_urls(plan.settings) == [YOUTUBE + "/"]


@pytest.mark.parametrize("scope", [
    {"kind": "rss"}, {"kind": "rss", "blog_feed": "external"},
])
async def test_blogs_refreshes_discovery_sources_without_adding_publishers(
    scope: dict[str, str],
) -> None:
    original = configured(
        SourceConfig("blog.test", ()),
        SourceConfig("news.ycombinator.com", ()),
        SourceConfig("x.com", ("alice",)), SourceConfig("reddit.com", ()),
        SourceConfig("youtube.com", ()),
    )
    plan = await resolve_scope(original, UnreachableTargets(), scope)

    assert plan.settings.sources == original.sources[:4]


async def test_selected_blog_refresh_does_not_collect_other_discovery_feeds(
) -> None:
    original = configured(
        SourceConfig("blog.test", ()), SourceConfig("x.com", ("alice",)),
    )
    plan = await resolve_scope(original, targets(), {
        "kind": "rss", "domains": ["blog.test"],
    })

    assert plan.settings.sources == original.sources[:1]
    assert plan.label == "blog.test"


@pytest.mark.parametrize("scope", [
    {"kind": "rss", "blog_feed": "unknown"},
    {"kind": "rss", "blog_feed": ""},
    {"kind": "all", "blog_feed": "external"},
])
async def test_invalid_blog_context_fails_before_reading_anything_saved(
    scope: object,
) -> None:
    with pytest.raises(InvalidInput):
        await resolve_scope(configured(), UnreachableTargets(), scope)


@pytest.mark.parametrize(("field", "values"), [
    ("domains", ["blog.test"]), ("domains", ["unconfigured.test"]),
    ("tags", ["tech"]),
])
async def test_external_refresh_rejects_publisher_filters(
    field: str, values: list[str],
) -> None:
    original = configured(
        SourceConfig("blog.test", (), tags=("tech",)),
        SourceConfig("x.com", ("alice",)),
    )

    with pytest.raises(InvalidInput, match="publisher domains or tags"):
        await resolve_scope(original, UnreachableTargets(), {
            "kind": "rss", "blog_feed": "external", field: values,
        })
