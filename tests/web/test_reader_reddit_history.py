import copy
import datetime as dt
import json
from typing import Any, NamedTuple
from unittest.mock import AsyncMock

import pytest

from dank.web.reader_reddit_history import preserve_reddit_archives

NOW = dt.datetime(2026, 10, 16, 20, tzinfo=dt.UTC)
OLDER = NOW - dt.timedelta(days=2)
VERSIONS = [("t3_post", round(NOW.timestamp() * 1000))]


class _Result(NamedTuple):
    rows: list[dict[str, Any]]


def _row(
    payload: dict[str, Any], timestamp: dt.datetime = OLDER,
) -> dict[str, Any]:
    return {
        "post_id": "t3_post",
        "scraped_at": timestamp,
        "payload": json.dumps(payload),
    }


@pytest.mark.parametrize("status", ["failed", "disabled"])
async def test_discussion_retains_old_capture_and_reports_latest_attempt(
    status: str,
) -> None:
    snapshots: dict[str, dict[str, Any]] = {
        "t3_post": {
            "title": "New title",
            "num_comments": 90,
            "detail_capture": {"status": "captured"},
            "comment_capture": {"status": status, "comments": []},
        },
    }
    untouched = copy.deepcopy(snapshots)
    client = AsyncMock()
    old_capture = {
        "status": "captured",
        "comments": [{"id": "t1_comment"}],
        "total_available": 3,
    }
    client.fetch_json.return_value = _Result(
        [_row({"comment_capture": old_capture})],
    )

    result = await preserve_reddit_archives(
        client, snapshots, VERSIONS, include_discussion=True,
    )

    assert snapshots == untouched
    assert result["t3_post"]["title"] == "New title"
    assert result["t3_post"]["num_comments"] == 90
    assert result["t3_post"]["comment_capture"] == {
        **old_capture,
        "captured_at": OLDER.isoformat(),
        "latest_attempt_status": status,
    }
    query, params = client.fetch_json.call_args.args
    assert "toUnixTimestamp64Milli(scraped_at) < %(before_0)s" in query
    assert params["before_0"] == VERSIONS[0][1]
    assert params["capture_field"] == "comment_capture"


async def test_failed_detail_recovers_media_and_keeps_failure_state() -> None:
    snapshots: dict[str, dict[str, Any]] = {
        "t3_post": {
            "detail_capture": {"status": "failed"},
            "media": [],
            "score": 100,
        },
    }
    media = [
        {
            "url": "https://v.redd.it/video/HLSPlaylist.m3u8",
            "asset_type": "video",
        },
    ]
    client = AsyncMock()
    client.fetch_json.return_value = _Result(
        [
            _row(
                {
                    "detail_capture": {"status": "captured"},
                    "media": media,
                    "score": 1,
                },
            ),
        ],
    )

    result = await preserve_reddit_archives(
        client, snapshots, VERSIONS, include_discussion=False,
    )

    assert result["t3_post"] == {
        "detail_capture": {"status": "failed"},
        "media": media,
        "score": 100,
    }
    assert snapshots["t3_post"]["media"] == []


@pytest.mark.parametrize(
    "snapshot",
    [
        {},
        {"comment_capture": {"status": "captured", "comments": []}},
        {"detail_capture": {"status": "captured"}, "media": []},
    ],
)
async def test_empty_and_historical_captures_never_revive_old_content(
    snapshot: dict[str, Any],
) -> None:
    client = AsyncMock()
    snapshots = {"t3_post": snapshot}

    assert (
        await preserve_reddit_archives(
            client, snapshots, VERSIONS, include_discussion=True,
        )
        == snapshots
    )
    client.fetch_json.assert_not_awaited()


async def test_feed_does_not_fetch_comment_history() -> None:
    client = AsyncMock()
    snapshots = {"t3_post": {"comment_capture": {"status": "failed"}}}

    assert (
        await preserve_reddit_archives(
            client, snapshots, VERSIONS, include_discussion=False,
        )
        == snapshots
    )
    client.fetch_json.assert_not_awaited()


@pytest.mark.parametrize("timestamp", [NOW, NOW + dt.timedelta(hours=1)])
async def test_newer_history_cannot_leak_into_processed_article(
    timestamp: dt.datetime,
) -> None:
    client = AsyncMock()
    client.fetch_json.return_value = _Result(
        [_row({"comment_capture": {"status": "captured"}}, timestamp)],
    )
    snapshots = {"t3_post": {"comment_capture": {"status": "failed"}}}

    assert (
        await preserve_reddit_archives(
            client, snapshots, VERSIONS, include_discussion=True,
        )
        == snapshots
    )
