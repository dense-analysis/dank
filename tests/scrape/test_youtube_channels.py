from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from dank.model import RawAsset
from dank.scrape import runner
from dank.scrape.youtube.channels import discover_channel
from dank.storage.clickhouse import QueryResult

CHANNEL = "UC" + "a" * 22
ICON = "https://yt3.ggpht.com/channel=s160-c-k-c0x00ffffff-no-rj"
FEED = "https://www.youtube.com/@Example/videos"
NOW = dt.datetime(2026, 10, 17, tzinfo=dt.UTC)


def initial(count: str = "26K videos") -> dict[str, Any]:
    return {
        "metadata": {"channelMetadataRenderer": {
            "externalId": CHANNEL, "title": "Example channel",
            "channelUrl": "https://www.youtube.com/channel/" + CHANNEL,
            "avatar": {"thumbnails": [{"url": ICON, "width": 160}]},
        }},
        "header": {"pageHeaderRenderer": {"content": {
            "pageHeaderViewModel": {"metadata": {
                "contentMetadataViewModel": {"metadataRows": [{
                    "metadataParts": [
                        {"text": {"content": "4.27M subscribers"}},
                        {"text": {"content": count}},
                    ],
                }]},
            }},
        }}},
        "contents": {"videoRenderer": {
            "channelThumbnailSupportedRenderers": {"url": "other-avatar"},
            "videoCountText": {"simpleText": "9 videos"},
        }},
    }


def discover(data: object):
    return discover_channel(data, feed_url=FEED, request_url=FEED + "?gl=GB",
                            observed_at=NOW)


@pytest.mark.parametrize(("count", "expected"), [
    ("26K videos", 26000), ("1.23M videos", 1230000),
    ("1,234 videos", 1234), ("1 video", 1), ("0 videos", 0),
    ("", None), ("4.27M subscribers", None),
])
def test_modern_channel_header_preserves_displayed_total(
    count: str, expected: int | None,
) -> None:
    channel = discover(initial(count))
    assert channel is not None
    assert channel.channel_id == CHANNEL
    assert channel.channel_name == "Example channel"
    assert channel.icon_url == ICON
    assert channel.reported_video_count == expected
    assert channel.reported_video_count_text == (
        count if expected is not None else ""
    )
    assert "contents" not in json.loads(channel.payload)
    assert channel.request_url == FEED + "?gl=GB"


def test_legacy_header_and_modern_decorated_avatar() -> None:
    data = initial()
    del data["metadata"]["channelMetadataRenderer"]["avatar"]
    data["header"] = {
        "c4TabbedHeaderRenderer": {
            "videosCountText": {"runs": [{"text": "123 videos"}]},
        },
        "decoratedAvatarViewModel": {"avatar": {"avatarViewModel": {
            "image": {"sources": [{"url": ICON}]},
        }}},
    }
    channel = discover(data)
    assert channel is not None
    assert channel.icon_url == ICON
    assert channel.reported_video_count == 123


def test_missing_metadata_never_infers_channel_from_video_creator() -> None:
    data = initial()
    del data["metadata"]
    assert discover(data) is None


@pytest.mark.parametrize("url", [
    "https://evil.test/icon.png", "https://yt3.ggpht.com.evil.test/icon.png",
    "https://user@yt3.ggpht.com/icon.png", "http://yt3.ggpht.com/icon.png",
    "https://yt3.ggpht.com:123/icon.png", "https://[",
])
def test_untrusted_avatar_hosts_do_not_become_downloads(url: str) -> None:
    data = initial()
    data["metadata"]["channelMetadataRenderer"]["avatar"] = {
        "thumbnails": [{"url": url}],
    }
    channel = discover(data)
    assert channel is not None
    assert channel.icon_url == ""


async def test_channel_persistence_caches_avatar_and_retains_raw_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel = discover(initial())
    assert channel is not None
    icon = tmp_path / "avatar.png"
    icon.write_bytes(b"\x89PNG\r\n\x1a\nimage")
    download = AsyncMock(return_value=[RawAsset(
        "youtube.com", "channels/" + CHANNEL, ICON, "image", NOW,
        "youtube-channel", str(icon),
    )])
    monkeypatch.setattr(runner, "download_assets", download)
    client = AsyncMock()
    await runner._persist_youtube_channels(  # pyright: ignore[reportPrivateUsage]
        client, Mock(), (channel,), assets_dir=tmp_path,
        max_asset_bytes=None, download_types=("image",), owner=None,
    )
    table, rows = client.insert_rows.call_args.args
    assert table == "youtube_channels"
    assert rows[0]["reported_video_count"] == 26000
    assert rows[0]["local_path"] == str(icon)
    assert rows[0]["content_type"] == "image/png"
    assert rows[0]["payload"] == channel.payload
    assert download.call_args.kwargs["max_asset_bytes"] == 1024 * 1024
    client.fetch_json.assert_not_awaited()


@pytest.mark.parametrize("mode", ["failed", "invalid_image", "disabled"])
async def test_metadata_refresh_keeps_previous_icon_when_refresh_unavailable(
    mode: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    channel = discover(initial("27K videos"))
    assert channel is not None
    invalid = tmp_path / "invalid"
    invalid.write_text("<html>Unavailable</html>")
    download = AsyncMock(return_value=([RawAsset(
        "youtube.com", "channels/" + CHANNEL, ICON, "image", NOW,
        "youtube-channel", str(invalid),
    )] if mode == "invalid_image" else []))
    monkeypatch.setattr(runner, "download_assets", download)
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([{
        "icon_url": ICON, "local_path": "/old/avatar.png",
        "content_type": "image/png",
    }])
    await runner._persist_youtube_channels(  # pyright: ignore[reportPrivateUsage]
        client, Mock(), (channel,), assets_dir=tmp_path,
        max_asset_bytes=None,
        download_types=() if mode == "disabled" else ("image",), owner=None,
    )
    row = client.insert_rows.call_args.args[1][0]
    assert row["reported_video_count"] == 27000
    assert row["local_path"] == "/old/avatar.png"

    if mode == "disabled":
        download.assert_not_awaited()
    elif mode == "invalid_image":
        assert not invalid.exists()


@pytest.mark.parametrize("after_consent", [False, True])
async def test_channel_header_is_emitted_before_video_batches(
    monkeypatch: pytest.MonkeyPatch, *, after_consent: bool,
) -> None:
    from dank.config import YouTubeSettings
    from dank.scrape.youtube import (
        _scrape_feed,  # pyright: ignore[reportPrivateUsage]
    )
    from dank.scrape.youtube.payloads import YouTubeFeed

    data = initial()
    data["contents"] = {"videoRenderer": {"videoId": "abcdefghijk"}}
    state = {"initial": data, "at_bottom": True}
    monkeypatch.setattr("dank.scrape.youtube._resolve_feed", AsyncMock(
        return_value=(FEED, {} if after_consent else state),
    ))
    snapshots = AsyncMock(return_value=state)
    monkeypatch.setattr("dank.scrape.youtube.snapshot", snapshots)
    monkeypatch.setattr("dank.scrape.youtube.asyncio.sleep", AsyncMock())
    capture = AsyncMock()
    capture.drain.return_value = []
    monkeypatch.setattr("dank.scrape.youtube.NetworkCapture",
                        Mock(return_value=capture))
    settings = YouTubeSettings()
    settings = settings._replace(scrape=settings.scrape._replace(
        max_scrolls=0,
    ))
    batches = [batch async for batch in _scrape_feed(
        AsyncMock(), YouTubeFeed(FEED, "Example", "channel", "videos"),
        settings, set(),
    )]
    assert len(batches[0].youtube_channels) == 1
    assert not batches[0].posts
    assert json.loads(batches[1].posts[0].payload)["channel_id"] == CHANNEL
    assert snapshots.await_count == int(after_consent)
