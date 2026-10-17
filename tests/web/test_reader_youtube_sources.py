from __future__ import annotations

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
