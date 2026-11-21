"""Map saved refresh targets from supplied query results."""

import pytest

from dank.api.refresh.clickhouse import ClickHouseRefreshTargets
from dank.api.refresh.model import ChannelFeed
from tests.api.clickhouse import query_client

VIDEO = "L_6pAVHYoFM"
FEED = "https://www.youtube.com/@Alice/videos"
CHANNEL = "UC1234567890123456789012"
CHANNEL_URL = "https://www.youtube.com/@Alice"


async def test_saved_playlists_answer_their_latest_title() -> None:
    selected = {"playlist_id": "PLone", "title": "New title"}
    targets = ClickHouseRefreshTargets(query_client(
        [selected, {"playlist_id": "PLtwo", "title": "Other"}],
        [selected], [],
    ))

    assert await targets.saved_playlists("") == {
        "PLone": "New title", "PLtwo": "Other",
    }
    assert await targets.saved_playlists("PLone") == {"PLone": "New title"}
    assert await targets.saved_playlists("PLunknown") == {}


async def test_saved_channel_rows_become_channel_feeds() -> None:
    found = await ClickHouseRefreshTargets(query_client([
        {"feed_url": FEED, "channel_id": CHANNEL, "channel_url": CHANNEL_URL},
        {
            "feed_url": "https://www.youtube.com/@Bob/videos",
            "channel_id": "UCbob",
            "channel_url": "https://www.youtube.com/@Bob",
        },
    ])).channel_feeds()

    assert found == [
        ChannelFeed(FEED, CHANNEL, CHANNEL_URL),
        ChannelFeed(
            "https://www.youtube.com/@Bob/videos",
            "UCbob",
            "https://www.youtube.com/@Bob",
        ),
    ]


async def test_saved_posts_are_known_by_domain_and_id() -> None:
    targets = ClickHouseRefreshTargets(query_client(
        [{"domain": "linked.test", "post_id": "article"}], [], [],
    ))

    assert await targets.has_post("linked.test", "article")
    assert not await targets.has_post("linked.test", "other")
    assert not await targets.has_post("other.test", "article")


@pytest.mark.parametrize("saved", [False, True])
async def test_video_availability_uses_saved_evidence(*, saved: bool) -> None:
    rows = [{"video_id": VIDEO}] if saved else []
    targets = ClickHouseRefreshTargets(query_client(rows))

    assert await targets.has_video(VIDEO) is saved
