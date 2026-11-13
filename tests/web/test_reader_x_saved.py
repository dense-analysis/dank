from __future__ import annotations

from typing import Any

import pytest
from starlette.datastructures import QueryParams
from test_reader_x_conversation import (
    archive,
    project,
    selected,
    tweet,
)

from dank.web.reader_query import (
    ReaderFilters,
    build_query,
    encode_cursor,
)
from dank.web.reader_x_conversation import connect_saved_posts
from dank.web.reader_x_saved import reply_metadata
from tests.web.query import parse_filters


def connect(
    target: dict[str, Any], nodes: list[dict[str, Any]],
    *, limited: bool = False,
) -> dict[str, Any]:
    result = project(target, target["rest_id"])
    connect_saved_posts(result, selected(target["rest_id"]), target, nodes, {},
                        limited=limited)

    return result


def test_posts_filter_before_pagination_by_displayed_version() -> None:
    filters = parse_filters(QueryParams(kind="x", account="author"))
    assert filters.x_view == "posts" and filters.sort == "newest"
    query, _ = build_query(filters, ())
    assert "in_reply_to_status_id_str" in query
    assert "toUnixTimestamp64Milli(updated_at)) NOT IN" in query
    assert query.index("NOT IN") < query.index("ORDER BY")
    assert query.index("ORDER BY") < query.index("LIMIT")
    query, _ = build_query(filters._replace(x_view="replies"), ())
    assert "in_reply_to_status_id_str" not in query
    assert "ORDER BY created_at DESC" in query


@pytest.mark.parametrize("query", [
    {"kind": "x", "x_view": "threads"},
    {"kind": "reddit", "x_view": "posts"},
])
def test_invalid_selection_is_rejected(query: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        parse_filters(QueryParams(query))


def test_cursor_cannot_cross_post_selections() -> None:
    filters = ReaderFilters(kind="x")
    cursor = encode_cursor(filters, selected().created_at, "x.com", "100")

    with pytest.raises(ValueError, match="cursor"):
        parse_filters(QueryParams(kind="x", x_view="replies", cursor=cursor))


def test_self_reply_uses_stable_ids_and_unknown_is_not_false() -> None:
    payload = tweet("101", "100")
    assert reply_metadata(payload)["reply_to"]["is_self"] is None
    payload["core"]["user_results"]["result"]["rest_id"] = "5"
    payload["legacy"]["in_reply_to_user_id_str"] = "5"
    assert reply_metadata(payload)["reply_to"]["is_self"] is True
    payload["legacy"]["in_reply_to_user_id_str"] = "6"
    assert reply_metadata(payload)["reply_to"]["is_self"] is False
    payload["legacy"]["in_reply_to_status_id_str"] = "invalid"
    assert "reply_to" not in reply_metadata(payload)


def test_saved_thread_keeps_short_replies_and_actual_branching() -> None:
    root = tweet("100", replies=10)
    result = connect(root, [
        tweet("103", "102", text="*purists. ;-)"),
        tweet("104", "100", handle="visitor"),
        tweet("102", "100", text="Three notes"),
        tweet("200", root="200"),
    ])
    assert [(p["id"], p["depth"]) for p in result["replies"]] == [
        ("102", 0), ("103", 1), ("104", 0),
    ]
    assert all(p["collected"] for p in result["replies"])
    assert "purists" in result["replies"][1]["html"]
    assert result["status"] == "not_collected"
    assert result["captured_at"] is None and result["captured_count"] == 0
    assert result["local_posts_count"] == 3


def test_saved_parent_chain_excludes_siblings_from_reply_detail() -> None:
    result = connect(tweet("102", "101"), [
        tweet("100"), tweet("101", "100"), tweet("103", "102"),
        tweet("104", "100"),
    ])
    assert [p["id"] for p in result["context"]] == ["100", "101"]
    assert [p["id"] for p in result["replies"]] == ["103"]
    assert not result["local_posts_limited"]


def test_saved_posts_deduplicate_archives_and_keep_archive_evidence() -> None:
    root = tweet("100", replies=2)
    root["conversation_capture"] = archive([
        tweet("101", "100", text="Archived body"),
    ], status="partial")
    original = project(root)
    result = connect(root, [
        tweet("101", "100", text="Later body"), tweet("102", "101"),
    ])
    assert [p["id"] for p in result["replies"]] == ["101", "102"]
    assert "Archived body" in result["replies"][0]["html"]
    assert result["replies"][0]["collected"]
    for field in ("status", "captured_at", "captured_count", "reason"):
        assert result[field] == original[field]
    assert result["local_posts_count"] == 1


def test_missing_parent_cycle_and_budget_never_claim_complete_thread() -> None:
    result = connect(tweet("102", "101"), [tweet("100")])
    assert result["context"] == [] and result["local_posts_limited"]
    result = connect(tweet("100"), [tweet("102", "101"), tweet("103", "102")])
    assert result["replies"][0]["missing_parent"]
    assert result["replies"][1]["depth"] == 1
    assert result["local_posts_limited"]
    result = connect(tweet("100"), [tweet("101", "102"), tweet("102", "101")])
    assert result["replies"] == [] and result["local_posts_limited"]
    result = connect(tweet("100"), [tweet("999")])
    assert result["replies"] == []
    result = connect(tweet("100"), [], limited=True)
    assert result["local_posts_limited"]
