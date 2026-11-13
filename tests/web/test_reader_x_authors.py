from __future__ import annotations

import datetime as dt

import pytest
from starlette.datastructures import QueryParams

from dank.api.posts.model import author_cursor, parse_author_cursor
from dank.web import (
    reader_x_authors as authors,
)
from dank.web.reader_query import build_query
from tests.web.query import parse_filters


def test_author_filter_stays_within_deduplicated_home_history():
    filters = parse_filters(QueryParams(
        kind="x", x_feed="for_you", account="@Alice", q="space",
        after="2026-10-01", before="2026-10-25", limit="1",
    ))
    snapshot = dt.datetime.now(dt.UTC).isoformat()
    query, params = build_query(
        filters, (), paginate=False, snapshot_time=snapshot,
    )
    assert "GROUP BY domain, post_id" in query
    assert "dank_x_home" in query and "LIMIT" not in query
    assert params["account"] == "alice"
    assert params["x_home_snapshot"] == snapshot
    assert params["term_0"] == "space"
    assert params["after"] == "2026-10-01 00:00:00"
    assert "in_reply_to_status_id_str" not in query


def test_author_cursor_keeps_cutoff_and_binds_search_and_handle():
    filters = parse_filters(QueryParams(kind="x", x_feed="for_you"))
    snapshot = dt.datetime.now(dt.UTC).isoformat()
    cursor = author_cursor(filters, snapshot, 5, "alice")
    assert parse_author_cursor(filters, cursor) == (
        snapshot, 5, "alice",
    )

    for changed in (filters._replace(q="space"),
                    filters._replace(account="alice")):
        with pytest.raises(ValueError, match="cursor"):
            parse_author_cursor(changed, cursor)

    sql, params = authors.author_query("saved_recommendations", (5, "alice"))
    assert "ORDER BY count DESC, handle ASC" in sql
    assert params == {"after_count": 5, "after_handle": "alice"}
