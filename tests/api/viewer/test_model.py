"""The viewer forgives queries it can't read, as it always has."""

import datetime as dt
from typing import Any

import pytest

from dank.api.viewer.model import (
    IndexFilters,
    days_back_label,
    index_filters,
    minimum_created_at,
)

NOW = dt.datetime(2026, 10, 30, 12, tzinfo=dt.UTC)


def filters(**query: Any) -> IndexFilters:
    return index_filters(query, page_size=50)


def test_an_empty_query_uses_the_defaults() -> None:
    assert filters() == IndexFilters(
        limit=50,
        search_text="",
        domain="",
        account="",
        days_back=0,
        cursor_created_at=None,
        cursor_post_id=None,
        cursor_direction="next",
    )


@pytest.mark.parametrize(("value", "limit"), [
    (None, 50),
    ("10", 10),
    ("200", 200),
    ("201", 200),
    ("0", 50),
    ("-1", 50),
    ("many", 50),
    ("", 50),
])
def test_limit_falls_back_and_is_capped(
    value: str | None, limit: int,
) -> None:
    assert filters(limit=value).limit == limit


@pytest.mark.parametrize(("value", "days"), [
    (None, 0),
    ("14", 14),
    ("-10", 0),
    ("many", 0),
    ("365", 365),
    ("366", 365),
])
def test_days_back_falls_back_and_is_capped(
    value: str | None, days: int,
) -> None:
    assert filters(days_back=value).days_back == days


def test_text_filters_are_trimmed() -> None:
    result = filters(q="  space  ", domain=" x.com ", account=" dense ")

    assert (result.search_text, result.domain, result.account) == (
        "space", "x.com", "dense",
    )


@pytest.mark.parametrize(("value", "expected"), [
    (None, None),
    ("", None),
    ("not a date", None),
    ("2026-10-30T12:00:00+00:00", NOW),
    ("2026-10-30T12:00:00Z", NOW),
    # No zone means UTC.
    ("2026-10-30T12:00:00", NOW),
])
def test_cursor_dates_are_read_as_utc_or_ignored(
    value: str | None, expected: dt.datetime | None,
) -> None:
    assert filters(cursor_created_at=value).cursor_created_at == expected


@pytest.mark.parametrize(("value", "direction"), [
    (None, "next"), ("next", "next"), ("prev", "prev"), ("sideways", "next"),
])
def test_only_prev_turns_the_pages_back(
    value: str | None, direction: str,
) -> None:
    assert filters(cursor_direction=value).cursor_direction == direction


def test_no_days_back_means_no_time_filter_and_shows_infinity() -> None:
    assert days_back_label(0) == "∞"
    assert minimum_created_at(0, NOW) is None


def test_days_back_counts_from_now() -> None:
    assert days_back_label(7) == "7"
    assert minimum_created_at(7, NOW) == NOW - dt.timedelta(days=7)
