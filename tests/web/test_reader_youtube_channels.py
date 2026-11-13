from __future__ import annotations

import datetime as dt

import pytest
from starlette.datastructures import QueryParams

from dank.api.youtube.model import parse_channel_cursor
from dank.config import SourceConfig, load_settings
from dank.web import reader_youtube_channels as channels
from dank.web.reader_youtube_query import (
    YouTubeFilters,
    build_youtube_query,
)
from dank.web.reader_youtube_sources import configured_channel_keys
from tests.web.query import parse_youtube_filters

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
        QueryParams(
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
        parse_youtube_filters(QueryParams({"channel_scope": "all-old-feeds"}))


def test_channel_paging_binds_filters_snapshot_and_stable_count_identity():
    filters = YouTubeFilters(feed=HOME, q="ambient", limit=20)
    stamp = dt.datetime.now(dt.UTC).isoformat()
    cursor = channels.channel_cursor(filters, stamp, 8, "UCexample")
    assert parse_channel_cursor(filters, cursor) == (
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
    assert "argMax(run_id, observed_at)" not in base
    assert "AND d.observed_at <=" in base
    assert params["snapshot_time"] == stamp
    with pytest.raises(ValueError):
        parse_channel_cursor(
            filters._replace(channel_scope="configured"), cursor,
        )
