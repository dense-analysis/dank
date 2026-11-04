"""ClickHouseRefreshTargets against a real, throwaway database."""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest

from dank.api.refresh.clickhouse import ClickHouseRefreshTargets
from dank.api.refresh.model import ChannelFeed
from dank.storage.clickhouse import ClickHouseClient
from tests.api.clickhouse import COLLECTED_AT, insert, raw_post

pytestmark = pytest.mark.clickhouse

VIDEO = "L_6pAVHYoFM"
FEED = "https://www.youtube.com/@Alice/videos"
CHANNEL = "UC1234567890123456789012"
CHANNEL_URL = "https://www.youtube.com/@Alice"


def playlist(
    playlist_id: str, title: str, checked_at: dt.datetime = COLLECTED_AT,
) -> dict[str, Any]:
    return {
        "playlist_id": playlist_id,
        "title": title,
        "checked_at": checked_at,
        "observed_at": checked_at,
    }


def channel(**fields: Any) -> dict[str, Any]:
    return {"observed_at": COLLECTED_AT, **fields}


def discovery(item_id: str, **fields: Any) -> dict[str, Any]:
    return {
        "run_id": "run",
        "item_id": item_id,
        "observed_at": COLLECTED_AT,
        "feed_url": FEED,
        **fields,
    }


async def test_saved_playlists_answer_their_latest_title(
    clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "youtube_playlists", [
        playlist("PLone", "Old title"),
        playlist("PLone", "New title", COLLECTED_AT + dt.timedelta(days=1)),
        playlist("PLtwo", "Other"),
    ])
    targets = ClickHouseRefreshTargets(clickhouse)

    assert await targets.saved_playlists("") == {
        "PLone": "New title", "PLtwo": "Other",
    }
    assert await targets.saved_playlists("PLone") == {"PLone": "New title"}
    assert await targets.saved_playlists("PLunknown") == {}


async def test_channel_feeds_come_from_channels_and_discoveries(
    clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "youtube_channels", [channel(
        channel_id=CHANNEL, channel_url=CHANNEL_URL, feed_url=FEED,
    )])
    await insert(clickhouse, "youtube_discoveries", [
        discovery(
            "video:one",
            feed_kind="channel",
            feed_url="https://www.youtube.com/@Bob/videos",
            payload=(
                '{"channel_id": "UCbob",'
                ' "channel_url": "https://www.youtube.com/@Bob"}'
            ),
        ),
        discovery("video:two", feed_kind="home", payload="{}"),
    ])
    found = await ClickHouseRefreshTargets(clickhouse).channel_feeds()

    assert sorted(found) == sorted([
        ChannelFeed(FEED, CHANNEL, CHANNEL_URL),
        ChannelFeed(
            "https://www.youtube.com/@Bob/videos",
            "UCbob",
            "https://www.youtube.com/@Bob",
        ),
    ])


async def test_saved_posts_are_known_by_domain_and_id(
    clickhouse: ClickHouseClient,
) -> None:
    await insert(clickhouse, "raw_posts", [
        raw_post("linked.test", "article", {}),
    ])
    targets = ClickHouseRefreshTargets(clickhouse)

    assert await targets.has_post("linked.test", "article")
    assert not await targets.has_post("linked.test", "other")
    assert not await targets.has_post("other.test", "article")


@pytest.mark.parametrize(("table", "row"), [
    ("youtube_reference_evidence", {
        "video_id": VIDEO, "domain": "blog.test", "post_id": "linking",
        "observed_at": COLLECTED_AT,
    }),
    ("youtube_discoveries", discovery("video:" + VIDEO, payload="{}")),
    ("raw_posts", raw_post("youtube.com", "video:" + VIDEO, {})),
])
async def test_videos_are_known_from_any_saved_evidence(
    clickhouse: ClickHouseClient, table: str, row: dict[str, Any],
) -> None:
    targets = ClickHouseRefreshTargets(clickhouse)

    assert not await targets.has_video(VIDEO)

    await insert(clickhouse, table, [row])

    assert await targets.has_video(VIDEO)
    assert not await targets.has_video("aaaaaaaaaaa")
