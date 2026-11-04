from __future__ import annotations

import pytest

from dank.config import SourceConfig, load_settings
from dank.web.reader_youtube_sources import configured_sources


def test_configured_feeds_and_channels_exist_before_first_collection() -> None:
    settings = load_settings("config.example.toml")._replace(sources=(
        SourceConfig("youtube.com", ("Channel4News",)),
    ))
    feeds, channels = configured_sources(settings, [])
    assert {feed["tab"] for feed in feeds if feed["kind"] == "news"} == {
        "top_stories", "business", "world",
    }
    assert any(feed["kind"] == "hype" for feed in feeds)
    assert channels == [{
        "id": "https://www.youtube.com/@Channel4News",
        "name": "@Channel4News",
        "url": "https://www.youtube.com/@Channel4News", "count": 0,
        "avatar_url": None, "reported_video_count": None,
        "reported_video_count_text": None,
    }]


def test_sidebar_keeps_collected_channels_without_listing_every_news_creator():
    settings = load_settings("config.example.toml")._replace(sources=())
    feeds, channels = configured_sources(settings, [
        {"feed_url": "https://www.youtube.com/feed/news_destination",
         "name": "News", "kind": "news", "region": "GB", "count": 20,
         "channel_id": "UCother", "channel_name": "Random publisher",
         "channel_url": "https://www.youtube.com/@Random"},
        {"feed_url": "https://www.youtube.com/@Channel4News/videos",
         "name": "@Channel4News", "kind": "channel", "region": "GB",
         "count": 5, "channel_id": "UC4News", "channel_name": "Channel 4 News",
         "channel_url": "https://www.youtube.com/@Channel4News"},
    ])
    assert len(channels) == 1
    assert channels[0]["id"] == "UC4News"
    assert channels[0]["count"] == 5
    assert any(
        feed["name"] == "News" and feed["count"] == 20 for feed in feeds
    )


def test_special_news_channel_and_legacy_channel_paths() -> None:
    settings = load_settings("config.example.toml")._replace(sources=(
        SourceConfig("youtube.com", (), (
            "https://www.youtube.com/channel/UCYfdidRxbB8Qhf0Nx7ioOYw/videos",
            "https://www.youtube.com/c/Example/community",
            "https://www.youtube.com/user/Reporter/videos",
        )),
    ))
    feeds, channels = configured_sources(settings, [])
    news = [feed for feed in feeds if feed["tab"] == "top_stories"]
    assert len(news) == 1
    assert news[0]["url"] == "https://www.youtube.com/feed/news_destination"
    assert {channel["url"] for channel in channels} == {
        "https://www.youtube.com/c/Example",
        "https://www.youtube.com/user/Reporter",
    }



def test_configured_hype_regions_are_distinct_before_collection() -> None:
    from dank.config import YouTubeSettings

    playlist = "https://www.youtube.com/playlist?list=US_LEADERBOARD"
    settings = load_settings("config.example.toml")._replace(
        sources=(SourceConfig("youtube.com", (), (playlist,)),),
        youtube=YouTubeSettings(hype_playlists=(("US", playlist),)),
    )
    feeds, _ = configured_sources(settings, [])
    hyped = [feed for feed in feeds if feed["kind"] == "hype"]
    assert {(feed["url"], feed["region"]) for feed in hyped} == {
        ("https://www.youtube.com/feed/hype", "GB"), (playlist, "US"),
    }
    assert all(feed["count"] == 0 for feed in hyped)

    feeds, _ = configured_sources(settings._replace(
        youtube=settings.youtube._replace(region="US"),
    ), [])
    assert [feed["url"] for feed in feeds if feed["kind"] == "hype"] == [
        playlist,
    ]


@pytest.mark.parametrize("explicit", [False, True])
def test_empty_recommendations_feed_is_visible_before_collection(
    *, explicit: bool,
) -> None:
    home = "https://www.youtube.com/"
    settings = load_settings("config.example.toml")
    settings = settings._replace(
        sources=(SourceConfig(
            "youtube.com", (), (home,) if explicit else (),
        ),),
        youtube=settings.youtube._replace(authenticated=True, homepage=True),
    )
    feeds, channels = configured_sources(settings, [])
    assert [feed for feed in feeds if feed["kind"] == "home"] == [{
        "id": home, "url": home, "name": "Recommendations", "kind": "home",
        "region": "GB", "tab": "", "count": 0,
    }]
    assert channels == []


@pytest.mark.parametrize("configured", [False, True])
@pytest.mark.parametrize("captured_name", [
    "Home recommendations", "Home", "Recommendations",
])
def test_recommendations_label_is_stable_for_saved_observations(
    *, configured: bool, captured_name: str,
) -> None:
    settings = load_settings("config.example.toml")
    settings = settings._replace(
        sources=(SourceConfig("youtube.com", ()),),
        youtube=settings.youtube._replace(
            authenticated=True, homepage=configured,
        ),
    )
    feeds, channels = configured_sources(settings, [{
        "feed_url": "https://www.youtube.com/", "name": captured_name,
        "kind": "home", "region": "US", "count": 42,
    }])
    home = [feed for feed in feeds if feed["kind"] == "home"]
    assert len(home) == 1
    assert home[0]["name"] == "Recommendations"
    assert home[0]["count"] == 42
    assert home[0]["region"] == "US"
    assert channels == []
