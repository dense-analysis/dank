from __future__ import annotations

import datetime as dt
import json
import pathlib
from typing import Any
from unittest.mock import AsyncMock

import pytest
from multidict import MultiDict

from dank.storage.clickhouse import QueryResult
from dank.web.reader_accounts import X_ACCOUNT_SQL, source_accounts
from dank.web.reader_query import (
    ReaderFilters,
    build_query,
    decode_cursor,
    encode_cursor,
    parse_filters,
)


@pytest.mark.parametrize("kind", ["", "all"])
def test_all_collections_keep_platforms_and_deduplicate_link_wrappers(
    kind: str,
) -> None:
    query, params = build_query(parse_filters(MultiDict({"kind": kind})), ())
    assert "(domain, post_id) NOT IN" in query
    assert "post_links" in query
    assert "x_domains" not in params


def test_stories_and_x_are_separate_collections() -> None:
    query, params = build_query(
        parse_filters(MultiDict({"kind": "x"})), (),
    )
    assert "domain IN %(x_domains)s" in query
    assert params["x_domains"] == ["x.com", "twitter.com"]
    stories, _ = build_query(
        parse_filters(MultiDict({"kind": "stories"})), (),
    )
    assert "WHERE 0" in stories


@pytest.mark.parametrize("mode", ["words", "meaning", "combined"])
def test_account_filter_intersects_search_author_and_dates(mode: str) -> None:
    filters = parse_filters(MultiDict({
        "kind": "x", "account": " @WatcherGuru ", "author": "Reporter",
        "q": "model", "mode": mode, "after": "2026-10-01",
    }))
    query, params = build_query(filters, (), (1.0, 0.0))
    assert filters.account == "watcherguru"
    assert f"{X_ACCOUNT_SQL} = %(account)s" in query
    assert params["account"] == "watcherguru"
    assert params["author"] == "Reporter"
    assert params["after"] == "2026-10-01 00:00:00"
    assert "watcherguru" not in query


@pytest.mark.parametrize("query", [
    {"account": "example"}, {"kind": "all", "account": "example"},
    {"kind": "stories", "account": "example"},
    {"kind": "reddit", "account": "example"},
    {"kind": "x", "account": "example/status/1"},
    {"kind": "x", "account": "' OR 1"},
    {"kind": "x", "account": "a" * 16},
    {"kind": "x", "community": "python"},
    {"kind": "x", "flair": "Discussion"},
])
def test_invalid_account_filters_are_rejected(query: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        parse_filters(MultiDict(query))


@pytest.mark.parametrize("changed", [
    {"account": "other"}, {"kind": "stories", "account": ""},
    {"kind": "all", "account": ""},
])
def test_account_cursor_cannot_cross_collections(
    changed: dict[str, str],
) -> None:
    filters = ReaderFilters(kind="x", account="example")
    token = encode_cursor(
        filters, dt.datetime(2026, 10, 1, tzinfo=dt.UTC), "x.com", "123",
    )

    with pytest.raises(ValueError, match="Invalid cursor"):
        decode_cursor(filters._replace(**changed, cursor=token))


async def test_account_identity_uses_committed_modern_x_profile() -> None:
    fixture = (
        pathlib.Path(__file__).parents[1] / "fixtures"
        / "UserTweetsWactherGuruExample.json"
    )
    captured = json.loads(fixture.read_text())
    # Use the Tweet's own user rather than quoted posts or suggestions.
    timeline = captured["data"]["user"]["result"]["timeline"]["timeline"]
    tweet = next(
        entry["content"]["itemContent"]["tweet_results"]["result"]
        for instruction in timeline["instructions"]
        for entry in instruction.get("entries", [])
        if entry["entryId"].startswith("tweet-")
    )
    profile = tweet["core"]["user_results"]["result"]
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([{"handle": "watcherguru", "count": 2}]),
        QueryResult([{
            "domain": "x.com", "handle": "watcherguru",
            "profile": json.dumps(profile),
        }]),
    ]
    accounts = await source_accounts(client, "x.com", ("@WatcherGuru",))
    assert accounts == [{
        "handle": "watcherguru", "name": "Watcher.Guru", "count": 2,
        "total_posts": 17848,
        "icon_url": "https://pbs.twimg.com/profile_images/1641221212578754562/DfiC0KW2_normal.png",
    }]
    assert X_ACCOUNT_SQL in client.fetch_json.call_args_list[0].args[0]
    assert X_ACCOUNT_SQL in client.fetch_json.call_args_list[1].args[0]


@pytest.mark.parametrize("profile,name,icon", [
    ({"legacy": {"screen_name": "EXAMPLE", "name": "Legacy name",
                 "profile_image_url_https": "https://pbs.twimg.com/profile_images/1/a.jpg"}},
     "Legacy name", "https://pbs.twimg.com/profile_images/1/a.jpg"),
    ({"core": {"screen_name": "other", "name": "Wrong account"}},
     "example", None),
    ({"core": None, "legacy": []}, "example", None),
    ([], "example", None),
])
async def test_account_profiles_handle_legacy_mismatched_and_malformed_data(
    profile: Any, name: str, icon: str | None,
) -> None:
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([]),
        QueryResult([{
            "domain": "x.com", "handle": "example",
            "profile": json.dumps(profile),
        }]),
    ]
    accounts = await source_accounts(client, "x.com", ("Example", "@EXAMPLE"))
    assert accounts == [{
        "handle": "example", "name": name, "count": 0, "icon_url": icon,
        "total_posts": None,
    }]


@pytest.mark.parametrize("icon", [
    "http://pbs.twimg.com/profile_images/1/a.jpg",
    "https://pbs.twimg.com.evil.example/profile_images/1/a.jpg",
    "https://pbs.twimg.com@evil.example/profile_images/1/a.jpg",
    "https://user@pbs.twimg.com/profile_images/1/a.jpg",
    "https://pbs.twimg.com:123/profile_images/1/a.jpg",
    "https://pbs.twimg.com/media/1.jpg",
    "https://pbs.twimg.com\n/profile_images/1/a.jpg",
    "javascript:alert(1)", "//pbs.twimg.com/profile_images/1/a.jpg", None,
])
async def test_account_icons_allow_only_https_x_profile_images(
    icon: object,
) -> None:
    client = AsyncMock()
    client.fetch_json.side_effect = [QueryResult([]), QueryResult([{
        "domain": "x.com", "handle": "example", "profile": json.dumps({
            "core": {"screen_name": "example", "name": "Example"},
            "avatar": {"image_url": icon},
        }),
    }])]
    accounts = await source_accounts(client, "x.com", ("example",))
    assert accounts[0]["name"] == "Example"
    assert accounts[0]["icon_url"] is None


async def test_unconfigured_x_source_keeps_historical_accounts() -> None:
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([{"handle": "archived", "count": 3}]), QueryResult([]),
    ]
    accounts = await source_accounts(client, "twitter.com", ())
    assert accounts == [{
        "handle": "archived", "name": "archived", "count": 3,
        "icon_url": None, "total_posts": None,
    }]


@pytest.mark.parametrize("counts,expected", [
    ({"tweet_counts": {"tweets": 10161}}, 10161),
    ({"tweet_counts": {"tweets": 0}}, 0),
    ({"legacy": {"statuses_count": 100}}, 100),
    ({"tweet_counts": {"tweets": 10161},
      "legacy": {"statuses_count": 100}}, 10161),
    ({}, None),
    ({"tweet_counts": []}, None),
    ({"tweet_counts": {"tweets": -1}}, None),
    ({"tweet_counts": {"tweets": True}}, None),
    ({"tweet_counts": {"tweets": "10161"}}, None),
    ({"tweet_counts": {"tweets": 1.5}}, None),
    ({"tweet_counts": {"tweets": 2**53}}, None),
])
async def test_account_totals_use_captured_profile_counts(
    counts: dict[str, Any], expected: int | None,
) -> None:
    client = AsyncMock()
    client.fetch_json.side_effect = [QueryResult([]), QueryResult([{
        "domain": "x.com", "handle": "example", "profile": json.dumps({
            "core": {"screen_name": "example", "name": "Example"},
            **counts,
        }),
    }])]
    accounts = await source_accounts(client, "x.com", ("example",))
    assert accounts[0]["total_posts"] == expected


async def test_account_total_cannot_come_from_another_profile() -> None:
    client = AsyncMock()
    client.fetch_json.side_effect = [QueryResult([]), QueryResult([{
        "domain": "x.com", "handle": "example", "profile": json.dumps({
            "core": {"screen_name": "other", "name": "Other"},
            "tweet_counts": {"tweets": 10161},
        }),
    }])]
    accounts = await source_accounts(client, "x.com", ("example",))
    assert accounts[0]["total_posts"] is None
