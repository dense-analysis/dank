from __future__ import annotations

import datetime as dt
from unittest.mock import AsyncMock

import pytest
from multidict import MultiDict

from dank.storage.clickhouse import QueryResult
from dank.web.reader_flairs import (
    FLAIR_WHITESPACE_PATTERN,
    LATEST_REDDIT_FLAIRS_SQL,
    MAX_FLAIR_LENGTH,
    reddit_flairs,
)
from dank.web.reader_query import (
    ReaderFilters,
    build_query,
    decode_cursor,
    encode_cursor,
    parse_filters,
)


def test_flairs_keep_case_and_normalize_repeated_labels() -> None:
    filters = parse_filters(MultiDict([
        ("kind", "reddit"), ("flair", "\u00a0\t New Model \u2003\n"),
        ("flair", "new model"), ("flair", "New Model"),
        ("flair", "🦙 Discussion"),
    ]))
    assert filters.flairs == ("New Model", "new model", "🦙 Discussion")
    assert parse_filters(MultiDict()).flairs == ()


@pytest.mark.parametrize("query", [
    [("flair", "New Model")],
    [("kind", "stories"), ("flair", "New Model")],
    [("kind", "reddit"), ("flair", "")],
    [("kind", "reddit"), ("flair", " \n\t")],
    [("kind", "reddit"), ("flair", "x" * (MAX_FLAIR_LENGTH + 1))],
    [("kind", "reddit"), *[("flair", "Discussion")] * 101],
])
def test_invalid_flair_filters_are_rejected(
    query: list[tuple[str, str]],
) -> None:
    with pytest.raises(ValueError, match=r"[Ff]lair"):
        parse_filters(MultiDict(query))


def test_flair_values_and_their_count_accept_the_boundaries() -> None:
    labels = [str(index) for index in range(99)] + ["x" * MAX_FLAIR_LENGTH]
    filters = parse_filters(MultiDict([
        ("kind", "reddit"), *(("flair", label) for label in labels),
    ]))
    assert len(filters.flairs) == 100


@pytest.mark.parametrize("mode", ["words", "meaning", "combined"])
def test_flair_group_intersects_community_search_dates_and_author(
    mode: str,
) -> None:
    filters = parse_filters(MultiDict([
        ("kind", "reddit"), ("community", "LocalLLaMA"),
        ("flair", "New Model"), ("flair", "O'Reilly 50% _news"),
        ("q", "Bonsai"), ("mode", mode), ("author", "Ada"),
        ("after", "2026-10-01"), ("before", "2026-10-15"),
    ]))
    query, params = build_query(filters, (), (1.0, 0.0))
    assert "domain = 'reddit.com' AND lowerUTF8" in query
    assert "= %(community)s AND post_id IN (" in query
    assert f"({LATEST_REDDIT_FLAIRS_SQL}) WHERE flair IN %(flairs)s)" in query
    assert params["community"] == "localllama"
    assert params["flairs"] == ["New Model", "O'Reilly 50% _news"]
    assert params["author"] == "Ada"
    assert params["after"] == "2026-10-01 00:00:00"
    assert params["before"] == "2026-10-16 00:00:00"
    assert "New Model" not in query and "O'Reilly" not in query


def test_filter_applies_after_the_latest_whole_snapshot_is_selected() -> None:
    query, params = build_query(
        ReaderFilters(kind="reddit", flairs=("New Model",)), (),
    )
    # Filtering raw captures before aggregation would revive historical flairs
    # when a newer capture clears, changes or malforms the flair value.
    assert "argMax(payload, scraped_at) AS latest_payload" in query
    assert "JSONType(latest_payload, 'link_flair_text') = 'String'" in query
    assert "GROUP BY post_id)) WHERE flair IN %(flairs)s" in query
    assert params["flairs"] == ["New Model"]
    assert query.count("%(flairs)s") == 1


def test_reddit_without_flair_does_not_query_raw_snapshots() -> None:
    query, params = build_query(ReaderFilters(kind="reddit"), ())
    assert "raw_posts" not in query
    assert "flairs" not in params


def test_flair_order_and_duplicates_do_not_change_cursor_identity() -> None:
    first = parse_filters(MultiDict([
        ("kind", "reddit"), ("flair", "New Model"), ("flair", "Discussion"),
    ]))
    reordered = parse_filters(MultiDict([
        ("kind", "reddit"), ("flair", "Discussion"), ("flair", "New Model"),
        ("flair", " Discussion "),
    ]))
    assert first.signature() == reordered.signature()
    created = dt.datetime(2026, 10, 15, 21, 9, 1, 123000, dt.UTC)
    cursor = encode_cursor(first, created, "reddit.com", "post")
    assert decode_cursor(reordered._replace(cursor=cursor)) == (
        created, "reddit.com", "post",
    )

    for labels in ((), ("New Model",), ("new model", "Discussion")):
        with pytest.raises(ValueError, match="Invalid cursor"):
            decode_cursor(first._replace(flairs=labels, cursor=cursor))


async def test_flair_facets_preserve_community_and_case_boundaries() -> None:
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([
        {"community": "accelerate", "name": "Discussion", "count": 2},
        {"community": "localllama", "name": "Discussion", "count": 5},
        {"community": "localllama", "name": "New Model", "count": 3},
        {"community": "localllama", "name": "new model", "count": 1},
    ])
    assert await reddit_flairs(client) == {
        "accelerate": [{"name": "Discussion", "count": 2}],
        "localllama": [
            {"name": "Discussion", "count": 5},
            {"name": "New Model", "count": 3},
            {"name": "new model", "count": 1},
        ],
    }
    query, params = client.fetch_json.call_args.args
    assert "FROM posts FINAL INNER JOIN" in query
    assert LATEST_REDDIT_FLAIRS_SQL in query
    assert "USING (post_id) WHERE domain = 'reddit.com'" in query
    assert "flair != ''" in query
    assert "GROUP BY community, name" in query
    assert params == {
        "max_flair_length": MAX_FLAIR_LENGTH,
        "flair_whitespace": FLAIR_WHITESPACE_PATTERN,
    }


async def test_no_available_flairs_returns_no_facets() -> None:
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([])
    assert await reddit_flairs(client) == {}
