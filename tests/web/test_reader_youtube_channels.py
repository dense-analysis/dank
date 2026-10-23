from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp.test_utils import make_mocked_request
from multidict import MultiDict

from dank.config import SourceConfig, load_settings
from dank.storage.clickhouse import QueryResult
from dank.web import reader_youtube_channels as channels
from dank.web.reader_youtube_query import (
    YouTubeFilters,
    build_youtube_query,
    parse_youtube_filters,
)
from dank.web.reader_youtube_sources import configured_channel_keys

HOME = "https://www.youtube.com/"


def test_configured_channels_use_explicit_configuration_and_captured_aliases():
    settings = load_settings("config.native.example.toml")._replace(
        sources=(
            SourceConfig(
                "youtube.com",
                ("Followed",),
                ("https://www.youtube.com/channel/UCexplicit/videos",),
            ),
        ),
    )
    keys = configured_channel_keys(
        settings,
        [
            {
                "channel_id": "UCfollowed",
                "channel_url": "https://www.youtube.com/@Followed",
                "feed_url": "https://www.youtube.com/@Followed/shorts",
            },
            {
                "channel_id": "UCretired",
                "channel_url": "https://www.youtube.com/@Retired",
                "feed_url": "https://www.youtube.com/@Retired/videos",
            },
        ],
    )
    assert {
        "UCfollowed",
        "UCexplicit",
        "https://www.youtube.com/@Followed",
    } <= set(keys)
    assert "UCretired" not in keys
    assert not any("Retired" in key for key in keys)


def test_configured_filter_is_applied_before_pagination():
    filters = parse_youtube_filters(
        MultiDict(
            {
                "feed": HOME,
                "channel_scope": "configured",
                "limit": "500",
            },
        ),
    )
    sql, params = build_youtube_query(filters, configured_channels=("UC1",))
    assert "IN %(configured)s" in sql
    assert sql.index("IN %(configured)s") < sql.index(
        "ORDER BY observed_at DESC",
    )
    assert params["configured"] == ("UC1",)
    assert params["limit"] == 501
    empty, _ = build_youtube_query(filters)
    assert "WHERE 0" in empty
    with pytest.raises(ValueError):
        parse_youtube_filters(MultiDict({"channel_scope": "all-old-feeds"}))


def test_channel_paging_binds_filters_snapshot_and_stable_count_identity():
    filters = YouTubeFilters(feed=HOME, q="ambient", limit=20)
    stamp = dt.datetime.now(dt.UTC).isoformat()
    cursor = channels.channel_cursor(filters, stamp, 8, "UCexample")
    assert channels.parse_channel_cursor(filters, cursor) == (
        stamp,
        8,
        "UCexample",
    )
    sql, params, base = channels.channel_query(
        filters, (), stamp, (8, "UCexample"),
    )
    assert "count < %(after_count)s" in sql
    assert "channel_key > %(after_key)s" in sql
    assert "ORDER BY count DESC, channel_key ASC" in sql
    assert "LIMIT" not in base
    assert "argMax(run_id, observed_at)" in base
    assert "AND d.observed_at <=" in base
    assert params["snapshot_time"] == stamp
    with pytest.raises(ValueError):
        channels.parse_channel_cursor(
            filters._replace(channel_scope="configured"), cursor,
        )


async def test_channel_endpoint_groups_saved_examples_and_reports_unknowns(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    settings = load_settings("config.native.example.toml")
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult(
            [
                {
                    "channel_key": "UC1",
                    "name": "Channel One",
                    "url": "",
                    "count": 8,
                    "video_count": 6,
                    "short_count": 2,
                    "post_count": 0,
                },
            ],
        ),
        QueryResult([{"total": 1, "unattributed_items": 4}]),
    ]
    monkeypatch.setattr(
        channels, "load_configured_channels", AsyncMock(return_value=("UC1",)),
    )
    previews = AsyncMock(return_value={"UC1": [{"id": "video:aaaaaaaaaaa"}]})
    monkeypatch.setattr(channels, "channel_previews", previews)
    request = make_mocked_request(
        "GET", "/api/reader/youtube/channels?feed=https://www.youtube.com/",
    )
    request.app["clickhouse"] = client
    request.app["state"] = SimpleNamespace(
        settings=settings, assets_dir=tmp_path,
    )
    response = await channels.handle_youtube_channels(request)
    assert response.status == 200
    body = json.loads(response.text or "")
    assert body["total"] == 1
    assert body["unattributed_items"] == 4
    assert body["channels"][0]["configured"] is True
    assert body["channels"][0]["items"][0]["id"] == "video:aaaaaaaaaaa"
    assert body["next_cursor"] is None
    assert previews.await_args is not None
    assert previews.await_args.args[4].feed == HOME


@pytest.mark.parametrize(
    "query",
    [
        "limit=51",
        "limit=0",
        "cursor=bad",
        "cursor=a&cursor=b",
        "id=video:x",
        "channel_scope=unknown",
        "unexpected=true",
    ],
)
async def test_channel_endpoint_rejects_invalid_requests_before_io(query: str):
    request = make_mocked_request(
        "GET", "/api/reader/youtube/channels?" + query,
    )
    response = await channels.handle_youtube_channels(request)
    assert response.status == 400
