from __future__ import annotations

import datetime as dt

import pytest
from multidict import MultiDict
from test_reader_refresh_scope import client, settings

from dank.config import SourceConfig
from dank.web.reader_query import build_query, parse_filters
from dank.web.reader_refresh_scope import resolve_scope
from dank.web.reader_x_home import decode_home_cursor, encode_home_cursor


def test_history_membership_preserves_captures_and_replies():
    filters = parse_filters(MultiDict(kind="x", x_feed="for_you"))
    query, params = build_query(filters, ())
    assert "dank_x_home" in query
    assert "GROUP BY domain, post_id" in query
    assert "max(scraped_at) AS recommended_at" in query
    assert "in_reply_to_status_id_str" not in query
    assert "ORDER BY recommended_at DESC, recommendation_position ASC" in query
    assert "x_home_snapshot" in params


def test_cursor_preserves_cutoff_and_position_and_cannot_cross_filters():
    filters = parse_filters(MultiDict(kind="x", x_feed="for_you"))
    snapshot = dt.datetime.now(dt.UTC).isoformat()
    cursor = encode_home_cursor(
        filters,
        {
            "recommended_at": "2026-10-20 10:00:00.123",
            "recommendation_position": 5,
            "domain": "x.com",
            "post_id": "123",
        },
        snapshot,
    )
    next_filters = parse_filters(
        MultiDict(kind="x", x_feed="for_you", cursor=cursor),
    )
    saved = decode_home_cursor(next_filters)
    assert saved.snapshot == snapshot and saved.position == 5
    query, params = build_query(next_filters, ())
    assert params["x_home_snapshot"] == snapshot
    assert params["x_home_position"] == -5
    assert "-toInt64(recommendation_position)" in query

    with pytest.raises(ValueError, match="cursor"):
        parse_filters(MultiDict(kind="x", cursor=cursor))

    with pytest.raises(ValueError, match="cursor"):
        parse_filters(
            MultiDict(
                kind="x", x_feed="for_you", cursor=cursor, q="different",
            ),
        )


@pytest.mark.parametrize(
    "query",
    [
        {"kind": "x", "x_feed": "following"},
        {"kind": "reddit", "x_feed": "for_you"},
        {"kind": "x", "x_feed": "for_you", "account": "invalid/handle"},
    ],
)
def test_rejects_ambiguous_or_wrong_feed(query: dict[str, str]):
    with pytest.raises(ValueError):
        parse_filters(MultiDict(query))


async def test_home_and_account_refresh_remain_separate():
    original = settings(SourceConfig("x.com", ("alice", "bob")))
    home = await resolve_scope(
        original, client(), {"kind": "x", "feed_id": "for_you"},
    )
    assert home.settings.x.homepage
    assert home.settings.sources[0].accounts == ()
    assert home.label == "X For You"
    account = await resolve_scope(
        original, client(), {"kind": "x", "account": "alice"},
    )
    assert not account.settings.x.homepage
    assert account.settings.sources[0].accounts == ("alice",)

    with pytest.raises(ValueError):
        await resolve_scope(
            original,
            client(),
            {"kind": "x", "feed_id": "for_you", "account": "alice"},
        )
