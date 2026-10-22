from __future__ import annotations

from pathlib import Path

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
        QueryResult([]),
        # One overlap between the two feeds, plus two unique News discoveries.
        QueryResult([{"id": "UC4News", "count": 9}]),
        QueryResult([{"total": 9}]), QueryResult([{"total": 0}]),
    ]
    request = make_mocked_request("GET", "/api/reader/youtube/sources", app={
        "clickhouse": client, "state": SimpleNamespace(
            settings=settings, assets_dir=Path("/tmp"),
        ),
    })
    response = await handle_youtube_sources(request)
    import json

    payload = json.loads(response.text or "{}")
    assert len(payload["channels"]) == 1
    assert payload["channels"][0]["count"] == 9
    query, params = client.fetch_json.call_args_list[2].args
    assert "uniqExact(item_id)" in query
    assert "arrayIntersect" in query
    assert "startsWith(item_id, 'video:')" in query
    assert params == {"ids": ["UC4News"]}



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


async def test_channel_metadata_is_local_and_preserves_rounded_source_count(
    tmp_path: Path,
) -> None:
    from unittest.mock import AsyncMock

    from dank.storage.clickhouse import QueryResult
    from dank.web.reader_youtube import (
        _channel_metadata,  # pyright: ignore[reportPrivateUsage]
    )

    icon = tmp_path / "youtube.com" / "channel.png"
    icon.parent.mkdir()
    icon.write_bytes(b"image")
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([{
        "channel_id": "UCexample", "channel_name": "Example channel",
        "channel_url": "https://www.youtube.com/channel/UCexample",
        "feed_url": "https://www.youtube.com/@Example/shorts",
        "icon_url": "https://yt3.ggpht.com/icon", "local_path": str(icon),
        "content_type": "image/png", "reported_video_count": "26000",
        "reported_video_count_text": "26K videos",
    }])
    metadata = await _channel_metadata(client, tmp_path)
    settings = load_settings("config.example.toml")._replace(sources=(
        SourceConfig("youtube.com", ("Example",)),
    ))
    _, channels = configured_sources(settings, [], metadata)
    assert channels[0]["id"] == "UCexample"
    assert channels[0]["name"] == "Example channel"
    assert channels[0]["avatar_url"] == "/assets/youtube.com/channel.png"
    assert channels[0]["reported_video_count"] == 26000
    assert channels[0]["reported_video_count_text"] == "26K videos"

    icon.unlink()
    metadata = await _channel_metadata(client, tmp_path)
    assert metadata[0]["avatar_url"] is None


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
