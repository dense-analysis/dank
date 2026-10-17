from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from dank.config import BrowserScrapeSettings, YouTubeSettings
from dank.scrape.youtube import (
    FeedCapture,
    _resolve_feed,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.youtube.payloads import (
    YouTubeFeed,
    discover_assets,
    hype_playlist,
    normalize_renderer,
    renderers,
)

FIXTURE = Path(__file__).parents[1] / "fixtures/youtube_listing_renderers.json"
NEWS = YouTubeFeed(
    "https://www.youtube.com/feed/news_destination", "News", "news",
)
NOW = datetime.datetime(2026, 10, 17, tzinfo=datetime.UTC)


def normalize(
    key: str,
    renderer: dict[str, Any],
    settings: YouTubeSettings | None = None,
):
    return normalize_renderer(
        key,
        renderer,
        feed=NEWS,
        shelf="Public news heading",
        rank=4,
        settings=settings or YouTubeSettings(),
        request_url=NEWS.url,
        observed_at=NOW,
    )


def test_actual_public_renderers_preserve_source_and_metadata() -> None:
    data = json.loads(FIXTURE.read_text())

    for key in ("videoRenderer", "postRenderer", "lockupViewModel",
                "shortsLockupViewModel"):
        post = normalize(key, data[key])
        assert post is not None
        payload = json.loads(post.payload)
        assert payload["title"]
        assert payload["thumbnail_url"].startswith("https://")
        assert payload["renderer"] == {key: data[key]}
        assert payload["discovery"]["shelf"] == "Public news heading"
        assert payload["discovery"]["observed_at"] == NOW.isoformat()
        assert payload["detail_capture"] == {"status": "disabled"}
        assert all(
            asset.asset_type not in {"video", "youtube"}
            for asset in discover_assets(
                post,
                YouTubeSettings(),
            )
        )

    assert hype_playlist(data).endswith(  # type: ignore[union-attr]
        "OLJbGgOZgwZ9k_XXE2tcr-JsD4tag1iKjng",
    )


@pytest.mark.parametrize(
    "metadata",
    [
        {"isLiveNow": True},
        {"upcomingEventData": {"startTime": "123"}},
        {
            "thumbnailOverlays": [
                {
                    "thumbnailOverlayTimeStatusRenderer": {
                        "style": "LIVE",
                        "text": {"simpleText": "LIVE"},
                    },
                },
            ],
        },
        {
            "contentImage": {
                "thumbnailBadgeViewModel": {
                    "badgeStyle": "THUMBNAIL_OVERLAY_BADGE_STYLE_LIVE",
                },
            },
        },
    ],
)
def test_livestreams_and_upcoming_videos_are_excluded(
    metadata: dict[str, object],
) -> None:
    assert (
        normalize(
            "videoRenderer",
            {
                "videoId": "abcdefghijk",
                "title": {"simpleText": "Some title"},
                **metadata,
            },
        )
        is None
    )


def test_shorts_toggle_does_not_treat_short_duration_as_a_short() -> None:
    video = {"videoId": "abcdefghijk", "lengthText": {"simpleText": "0:13"}}
    regular = normalize(
        "videoRenderer", video, YouTubeSettings(include_shorts=False),
    )
    assert regular is not None
    assert json.loads(regular.payload)["kind"] == "video"
    assert (
        normalize(
            "reelItemRenderer", video, YouTubeSettings(include_shorts=False),
        )
        is None
    )
    short = normalize(
        "reelItemRenderer", video, YouTubeSettings(scrape_videos=True),
    )
    assert short is not None
    assert json.loads(short.payload)["kind"] == "short"
    assert (
        discover_assets(short, YouTubeSettings(scrape_videos=True))[
            -1
        ].asset_type
        == "youtube"
    )


def test_shelves_survive_while_network_echo_is_deduplicated() -> None:
    renderer = {"videoId": "abcdefghijk"}
    capture = FeedCapture(NEWS, YouTubeSettings(), set())
    batch = capture.extract(
        [
            ("videoRenderer", renderer, "Economy"),
            ("videoRenderer", renderer, "World"),
            ("videoRenderer", renderer, "Economy"),
            ("videoRenderer", renderer, ""),
        ],
        NEWS.url,
    )
    assert len(batch.posts) == 2
    assert {
        json.loads(post.payload)["discovery"]["shelf"] for post in batch.posts
    } == {"Economy", "World"}


def test_overlap_only_stops_chronological_channels() -> None:
    ids = ["aaaaaaaaaaa", "bbbbbbbbbbb", "ccccccccccc", "ddddddddddd"]
    known = {f"video:{item}" for item in ids[1:]}
    settings = YouTubeSettings(known_overlap=2)
    candidates = [("videoRenderer", {"videoId": item}, "") for item in ids]
    channel = YouTubeFeed(
        "https://www.youtube.com/@news/videos", "News", "channel", "videos",
    )
    captured = FeedCapture(channel, settings, known)
    assert len(captured.extract(candidates, channel.url).posts) == 3
    assert captured.reached_overlap
    news = FeedCapture(NEWS, settings, known)
    assert len(news.extract(candidates, NEWS.url).posts) == 4
    assert not news.reached_overlap


def test_shelf_heading_is_retained_through_nested_rich_items() -> None:
    value = {
        "richShelfRenderer": {
            "title": {"runs": [{"text": "Economy"}]},
            "contents": [
                {
                    "richItemRenderer": {
                        "content": {
                            "videoRenderer": {"videoId": "abcdefghijk"},
                        },
                    },
                },
            ],
        },
    }
    assert list(renderers(value)) == [
        ("videoRenderer", {"videoId": "abcdefghijk"}, "Economy"),
    ]


async def test_hype_follows_actual_view_all_playlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = json.loads(FIXTURE.read_text())
    navigation = AsyncMock()
    states = AsyncMock(side_effect=[{"initial": data}, {"initial": {}}])
    monkeypatch.setattr("dank.scrape.youtube.navigate", navigation)
    monkeypatch.setattr("dank.scrape.youtube.snapshot", states)
    feed = YouTubeFeed("https://www.youtube.com/feed/hype", "Hype", "hype")
    url, _ = await _resolve_feed(AsyncMock(), feed, YouTubeSettings())
    assert "list=OLJbGgOZgwZ9k_XXE2tcr-JsD4tag1iKjng" in url
    assert navigation.await_count == 2
    assert "/playlist?list=" in navigation.await_args_list[-1].args[1]


def test_listing_limit_counts_items_without_losing_multiple_discoveries() -> (
    None
):
    capture = FeedCapture(
        NEWS,
        YouTubeSettings(
            scrape=BrowserScrapeSettings(max_posts=1),
        ),
        set(),
    )
    posts = capture.extract(
        [
            ("videoRenderer", {"videoId": "aaaaaaaaaaa"}, "World"),
            ("videoRenderer", {"videoId": "bbbbbbbbbbb"}, "World"),
            ("videoRenderer", {"videoId": "aaaaaaaaaaa"}, "Economy"),
        ],
        NEWS.url,
    ).posts
    assert len(posts) == 2
    assert {post.post_id for post in posts} == {"video:aaaaaaaaaaa"}


def test_post_links_expand_short_display_urls_without_fetching() -> None:
    post = normalize("postRenderer", {
        "postId": "UgExample",
        "contentText": {"runs": [{"text": "See "}, {
            "text": "https://news.example/short...",
            "navigationEndpoint": {"urlEndpoint": {
                "url": "https://www.youtube.com/redirect?q="
                       "https%3A%2F%2Fnews.example%2Ffull-article",
            }},
        }]},
    })
    assert post is not None
    assert json.loads(post.payload)["text"] == (
        "See https://news.example/full-article"
    )


def test_global_positions_preserve_shelves_across_batches() -> None:
    capture = FeedCapture(NEWS, YouTubeSettings(), set())
    first = capture.extract([
        ("videoRenderer", {"videoId": "aaaaaaaaaaa"}, "World"),
        ("videoRenderer", {"videoId": "bbbbbbbbbbb"}, "World"),
    ], NEWS.url)
    second = capture.extract([
        ("videoRenderer", {"videoId": "aaaaaaaaaaa"}, "World"),
        ("videoRenderer", {"videoId": "ccccccccccc"}, "Economy"),
    ], NEWS.url)
    context = [json.loads(post.payload)["discovery"]
               for post in [*first.posts, *second.posts]]
    assert [item["position"] for item in context] == [1, 2, 3]
    assert [item["rank"] for item in context] == [1, 2, 1]
    timestamps = {post.scraped_at for post in [*first.posts, *second.posts]}
    assert len(timestamps) == 1


async def test_one_failed_feed_keeps_other_feeds_and_reports_partial_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dank.config import SourceConfig, load_settings
    from dank.scrape.metrics import SourceMetrics, current_source
    from dank.scrape.types import ScrapeBatch
    from dank.scrape.youtube import YouTubeFeedError, scrape_youtube_feeds

    fixture = tmp_path / "settings.fixture.toml"
    fixture.write_text("")
    settings = load_settings(fixture)._replace(youtube=YouTubeSettings(
        hype=False, news_tabs=("top_stories", "business"),
    ))
    source = SourceConfig("youtube.com", ())
    statistics = SourceMetrics(0, source)
    page = AsyncMock()
    browser = AsyncMock()
    browser.get.return_value = page
    session = AsyncMock()
    session.get_browser.return_value = browser

    async def collect(*args: Any):
        feed = args[1]

        if feed.tab == "top_stories":
            raise YouTubeFeedError("Unavailable first listing")

        post = normalize("videoRenderer", {"videoId": "abcdefghijk"})
        assert post is not None
        yield ScrapeBatch([post], [])

    monkeypatch.setattr("dank.scrape.youtube._scrape_feed", collect)
    token = current_source.set(statistics)

    try:
        batches = [batch async for batch in scrape_youtube_feeds(
            settings, source, session,
        )]
    finally:
        current_source.reset(token)

    assert len(batches) == 1
    assert statistics.counts["fetch_failures"] == 1
    assert statistics.error_type == "YouTubeFeedError"
    assert len(statistics.feed_urls) == 2
    page.close.assert_awaited_once()


def test_sorted_channel_feeds_do_not_use_chronological_overlap() -> None:
    feed = YouTubeFeed("https://www.youtube.com/@news/videos?sort=p",
                       "Popular", "channel", "videos")
    capture = FeedCapture(feed, YouTubeSettings(known_overlap=1),
                          {"video:aaaaaaaaaaa"})
    batch = capture.extract([
        ("videoRenderer", {"videoId": "aaaaaaaaaaa"}, ""),
        ("videoRenderer", {"videoId": "bbbbbbbbbbb"}, ""),
    ], feed.url)
    assert len(batch.posts) == 2
    assert not capture.reached_overlap



async def test_hype_stops_after_top_100_without_extra_scrolls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dank.scrape.youtube import (
        _scrape_feed,  # pyright: ignore[reportPrivateUsage]
    )

    feed = YouTubeFeed("https://www.youtube.com/feed/hype", "Hype", "hype")
    data = [{"videoRenderer": {"videoId": f"{index:011d}"}}
            for index in range(100)]
    monkeypatch.setattr("dank.scrape.youtube._resolve_feed", AsyncMock(
        return_value=("https://www.youtube.com/playlist?list=test", {
            "initial": data, "at_bottom": False,
        }),
    ))
    capture = AsyncMock()
    capture.drain.return_value = []
    monkeypatch.setattr("dank.scrape.youtube.NetworkCapture",
                        Mock(return_value=capture))
    page = AsyncMock()
    batches = [batch async for batch in _scrape_feed(
        page, feed, YouTubeSettings(), set(),
    )]
    assert sum(len(batch.posts) for batch in batches) == 100
    page.evaluate.assert_not_awaited()
    capture.stop.assert_awaited_once()
