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


async def test_channel_counts_match_unique_items_across_all_feed_discoveries():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from aiohttp.test_utils import make_mocked_request

    from dank.storage.clickhouse import QueryResult
    from dank.web.reader_youtube import handle_youtube_sources

    settings = load_settings("config.example.toml")._replace(sources=(
        SourceConfig("youtube.com", (), (
            "https://www.youtube.com/@Channel4News/videos",
            "https://www.youtube.com/@Channel4News/shorts",
        )),
    ))
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([
            {"feed_url": "https://www.youtube.com/@Channel4News/" + tab,
             "name": "@Channel4News", "kind": "channel", "region": "GB",
             "count": count, "channel_id": "UC4News",
             "channel_name": "Channel 4 News",
             "channel_url": "https://www.youtube.com/@Channel4News"}
            for tab, count in (("videos", 5), ("shorts", 3))
        ]),
        # One overlap between the two feeds, plus two unique News discoveries.
        QueryResult([{"id": "UC4News", "count": 9}]),
        QueryResult([{"total": 9}]), QueryResult([{"total": 0}]),
    ]
    request = make_mocked_request("GET", "/api/reader/youtube/sources", app={
        "clickhouse": client, "state": SimpleNamespace(settings=settings),
    })
    response = await handle_youtube_sources(request)
    import json

    payload = json.loads(response.text or "{}")
    assert len(payload["channels"]) == 1
    assert payload["channels"][0]["count"] == 9
    query, params = client.fetch_json.call_args_list[1].args
    assert "uniqExact(item_id)" in query
    assert "arrayIntersect" in query
    assert params == {"ids": ["UC4News"]}
