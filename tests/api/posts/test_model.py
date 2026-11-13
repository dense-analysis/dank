"""Filter and cursor rules, with plain values."""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest

from dank.api.posts.model import (
    AUTHOR_PAGE_LIMIT,
    FilterInput,
    ListedPost,
    ReaderFilters,
    Recommendation,
    author_cursor,
    decode_cursor,
    decode_home_cursor,
    encode_cursor,
    feed_snapshot,
    next_cursor,
    parse_author_cursor,
    parse_limit,
    reader_filters,
    require_author_feed,
)
from dank.api.posts.schemas import HackerNewsRanking, PostsQuery, XAuthorsQuery
from dank.config import HackerNewsFilter
from dank.errors import DankError, InvalidInput
from tests.api.posts.fakes import NOW, row


def test_invalid_input_is_a_value_error() -> None:
    assert issubclass(InvalidInput, DankError)
    assert issubclass(InvalidInput, ValueError)


def test_filters_normalize_what_a_request_sent() -> None:
    filters = reader_filters(FilterInput(
        q="  space  ", domains=(" B.example ", "a.example", "a.EXAMPLE"),
        tags=("Politics",), author=" O'Reilly ", after="2026-10-01",
        before="2026-10-08", limit="5",
    ))

    assert filters == ReaderFilters(
        q="space", domains=("a.example", "b.example"), tags=("politics",),
        author="O'Reilly", limit=5,
        after=dt.datetime(2026, 10, 1, tzinfo=dt.UTC),
        before=dt.datetime(2026, 10, 8, tzinfo=dt.UTC),
    )


def test_collection_filters_normalize_too() -> None:
    reddit = reader_filters(FilterInput(
        kind="reddit", community=" LocalLLaMA ", flairs=(" News ", "News"),
    ))
    x = reader_filters(FilterInput(kind="x", account=" @Alice "))
    hacker_news = reader_filters(FilterInput(kind="hacker_news"))

    assert (reddit.community, reddit.flairs) == ("localllama", ("News",))
    assert x.account == "alice"
    assert hacker_news.hn_filter == "top50"
    assert reader_filters(FilterInput()).hn_filter == ""
    assert reader_filters(FilterInput()).x_view == "posts"


@pytest.mark.parametrize("raw", [
    FilterInput(hn_filter=""), FilterInput(kind="hacker_news", hn_filter=""),
    FilterInput(x_view=""), FilterInput(kind="x", x_view=""),
    FilterInput(blog_feed=""), FilterInput(kind="stories", blog_feed=""),
    FilterInput(mode="typo"), FilterInput(sort="random"),
    FilterInput(author_match="typo"), FilterInput(kind="unknown"),
    FilterInput(sort="relevance"), FilterInput(q="w " * 51),
    FilterInput(q="w" * 2001), FilterInput(author="a" * 301),
    FilterInput(domains=("a" * 254,)), FilterInput(tags=("",)),
    FilterInput(domains=("a",) * 101), FilterInput(platforms=("x",) * 101),
    FilterInput(kind="reddit", flairs=("f" * 301,)),
    FilterInput(after="1899-12-31"), FilterInput(before="2299-01-01"),
    FilterInput(after="2026-1-1"), FilterInput(after="20261008"),
    FilterInput(after="2026-10-09", before="2026-10-08"),
    FilterInput(cursor="bad"), FilterInput(limit="oops"), FilterInput(limit=0),
    FilterInput(kind="x", x_feed="for_you", cursor="bad"),
])
def test_rules_reject_with_invalid_input(raw: FilterInput) -> None:
    with pytest.raises(InvalidInput):
        reader_filters(raw)


@pytest.mark.parametrize("value", ["+5", " 5", "5 ", "1_0", 5])
def test_limit_keeps_pythons_integer_syntax(value: object) -> None:
    assert parse_limit(value) in {5, 10}


@pytest.mark.parametrize("value", ["5.0", "", "1e1", None, "0", "101"])
def test_limit_rejects_everything_else(value: object) -> None:
    with pytest.raises(InvalidInput):
        parse_limit(value)


def test_cursor_keeps_timestamp_precision_and_identity() -> None:
    filters = ReaderFilters(limit=2)
    created = dt.datetime(2026, 10, 8, 12, 30, 45, 123000, tzinfo=dt.UTC)

    cursor = encode_cursor(filters, created, "a.example", "1")

    assert decode_cursor(filters._replace(cursor=cursor)) == (
        created, "a.example", "1",
    )


def test_page_size_can_change_within_a_cursor_session() -> None:
    cursor = encode_cursor(ReaderFilters(limit=2), NOW, "a.example", "1")

    assert decode_cursor(ReaderFilters(limit=30, cursor=cursor))[1:] == (
        "a.example", "1",
    )


@pytest.mark.parametrize("change", [
    {"q": "space"}, {"kind": "reddit"}, {"author_match": "exact"},
    {"domains": ("a.example",)},
])
def test_cursor_cannot_cross_filters(change: dict[str, Any]) -> None:
    cursor = encode_cursor(ReaderFilters(), NOW, "a.example", "1")

    with pytest.raises(InvalidInput, match="Invalid cursor"):
        decode_cursor(ReaderFilters(cursor=cursor)._replace(**change))


def test_naive_cursor_timestamps_mean_utc() -> None:
    naive = NOW.replace(tzinfo=None)
    filters = ReaderFilters()

    cursor = encode_cursor(filters, naive, "a.example", "1")

    assert decode_cursor(filters._replace(cursor=cursor))[0] == NOW


def test_next_cursor_uses_the_kind_of_listing() -> None:
    plain = ListedPost(row("a.example", "1"))
    home = ListedPost(
        row("x.com", "2"), Recommendation("2026-10-20 10:00:00.123", 5),
    )
    snapshot = NOW.isoformat()
    feed = ReaderFilters(kind="x", x_feed="for_you")

    plain_cursor = next_cursor(ReaderFilters(), plain, snapshot)
    home_cursor = next_cursor(feed, home, snapshot)

    assert plain_cursor is not None
    assert home_cursor is not None
    assert decode_cursor(
        ReaderFilters(cursor=plain_cursor),
    )[1:] == ("a.example", "1")
    assert decode_home_cursor(feed._replace(cursor=home_cursor))[:3] == (
        snapshot, "2026-10-20T10:00:00.123+00:00", 5,
    )
    assert next_cursor(
        ReaderFilters(q="a", sort="relevance"), plain, snapshot,
    ) is None


def test_a_for_you_session_keeps_its_first_snapshot() -> None:
    feed = ReaderFilters(kind="x", x_feed="for_you")
    home = ListedPost(
        row("x.com", "2"), Recommendation("2026-10-20 10:00:00", 5),
    )
    later = NOW + dt.timedelta(hours=1)
    cursor = next_cursor(feed, home, NOW.isoformat())

    assert cursor is not None
    assert feed_snapshot(feed, later) == later.isoformat()
    assert feed_snapshot(
        feed._replace(cursor=cursor), later,
    ) == NOW.isoformat()


def test_author_cursor_keeps_its_cutoff_and_binds_filters() -> None:
    filters = ReaderFilters(kind="x", x_feed="for_you")
    cursor = author_cursor(filters, NOW.isoformat(), 5, "alice")

    assert parse_author_cursor(filters, cursor) == (
        NOW.isoformat(), 5, "alice",
    )

    for changed in (filters._replace(q="space"),
                    filters._replace(account="alice")):
        with pytest.raises(InvalidInput, match="cursor"):
            parse_author_cursor(changed, cursor)


@pytest.mark.parametrize("filters", [
    ReaderFilters(),
    ReaderFilters(kind="x"),
    ReaderFilters(kind="x", x_feed="for_you", limit=AUTHOR_PAGE_LIMIT + 1),
])
def test_authors_need_the_for_you_feed_and_a_small_page(
    filters: ReaderFilters,
) -> None:
    with pytest.raises(InvalidInput, match="Authors require"):
        require_author_feed(filters)


def test_query_models_hand_every_filter_to_the_rules() -> None:
    query = PostsQuery.model_validate({
        "q": " space ", "mode": "combined", "sort": "oldest",
        "domain": ["A.example"], "tag": ["Politics"], "platform": ["x"],
        "author": "Reporter", "author_match": "exact",
        "after": "2026-10-01", "before": "2026-10-02", "limit": "5",
        "kind": "all",
    })

    assert query.filters() == ReaderFilters(
        q="space", mode="combined", sort="oldest", domains=("a.example",),
        tags=("politics",), platforms=("x",), author="Reporter",
        author_match="exact", limit=5, kind="all",
        after=dt.datetime(2026, 10, 1, tzinfo=dt.UTC),
        before=dt.datetime(2026, 10, 2, tzinfo=dt.UTC),
    )


def test_author_cursors_are_not_read_as_post_cursors() -> None:
    query = XAuthorsQuery.model_validate({
        "kind": "x", "x_feed": "for_you", "cursor": "not-a-post-cursor",
    })

    assert query.filters().cursor == ""
    assert query.filters().limit == 20
    assert query.cursor == "not-a-post-cursor"


def test_the_ranking_type_matches_the_configured_rankings() -> None:
    assert set(HackerNewsRanking.__args__) == set(HackerNewsFilter)
