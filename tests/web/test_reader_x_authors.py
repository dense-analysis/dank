from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp.test_utils import make_mocked_request
from multidict import MultiDict

from dank.config import load_settings
from dank.storage.clickhouse import QueryResult
from dank.web import (
    reader_api,
    reader_x_authors as authors,
)
from dank.web.reader_query import build_query, parse_filters


def test_author_filter_stays_within_deduplicated_home_history():
    filters = parse_filters(MultiDict(
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
    filters = parse_filters(MultiDict(kind="x", x_feed="for_you"))
    snapshot = dt.datetime.now(dt.UTC).isoformat()
    cursor = authors.author_cursor(filters, snapshot, 5, "alice")
    assert authors.parse_author_cursor(filters, cursor) == (
        snapshot, 5, "alice",
    )

    for changed in (filters._replace(q="space"),
                    filters._replace(account="alice")):
        with pytest.raises(ValueError, match="cursor"):
            authors.parse_author_cursor(changed, cursor)

    sql, params = authors.author_query("saved_recommendations", (5, "alice"))
    assert "ORDER BY count DESC, handle ASC" in sql
    assert params == {"after_count": 5, "after_handle": "alice"}


async def test_endpoint_disambiguates_names_and_retains_paging(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([
            {"handle": "alice", "name": "Shared name", "count": 7},
            {"handle": "bob", "name": "Shared name", "count": 3},
        ]),
        QueryResult([{"total": 2, "unattributed_posts": 1}]),
    ]
    previews = AsyncMock(return_value={"alice": [{
        "id": "10", "domain": "x.com",
        "x": {"name": "Shared name", "icon_url": None},
    }]})
    monkeypatch.setattr(authors, "author_examples", previews)
    request = make_mocked_request(
        "GET", "/api/reader/x/authors?kind=x&x_feed=for_you&limit=1",
    )
    request.app["clickhouse"] = client
    request.app["state"] = SimpleNamespace(
        settings=load_settings("config.example.toml"), assets_dir=tmp_path,
    )
    response = await authors.handle_x_authors(request)
    assert response.status == 200
    body = json.loads(response.text or "")
    assert body["total"] == 2 and body["unattributed_posts"] == 1
    assert body["authors"][0]["handle"] == "alice"
    assert body["authors"][0]["count"] == 7
    assert body["authors"][0]["posts"][0]["id"] == "10"
    filters = parse_filters(MultiDict(kind="x", x_feed="for_you", limit="1"))
    snapshot, count, handle = authors.parse_author_cursor(
        filters, body["next_cursor"],
    )
    assert count == 7 and handle == "alice"
    assert client.fetch_json.call_args_list[0].args[1][
        "x_home_snapshot"
    ] == snapshot
    assert previews.call_args.args[1] == [
        {"handle": "alice", "name": "Shared name", "count": 7},
    ]


async def test_examples_are_bounded_and_reuse_saved_content_adapter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    from test_reader import row

    first = {**row("x.com", "10"), "handle": "alice"}
    second = {**row("x.com", "11"), "handle": "bob"}
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([first, second])
    adapter = AsyncMock(return_value=[
        {"id": "10", "domain": "x.com"},
        {"id": "11", "domain": "x.com"},
    ])
    monkeypatch.setattr(reader_api, "_post_payloads", adapter)
    request = make_mocked_request("GET", "/api/reader/x/authors")
    request.app["clickhouse"] = client
    request.app["state"] = SimpleNamespace(assets_dir=tmp_path)
    result = await authors.author_examples(
        request, [{"handle": "alice"}, {"handle": "bob"}],
        "saved_posts", {}, "space",
    )
    assert result == {"alice": [{"id": "10", "domain": "x.com"}],
                      "bob": [{"id": "11", "domain": "x.com"}]}
    sql, params = client.fetch_json.call_args.args
    assert "LIMIT %(examples)s BY handle" in sql
    assert params == {"author_handles": ("alice", "bob"), "examples": 3}
    assert adapter.call_args.args[3] == "space"


@pytest.mark.parametrize("query", [
    "", "kind=x", "kind=reddit&x_feed=for_you",
    "kind=x&x_feed=for_you&limit=51",
    "kind=x&x_feed=for_you&cursor=bad",
    "kind=x&x_feed=for_you&cursor=a&cursor=b",
    "kind=x&x_feed=for_you&account=invalid/handle",
])
async def test_invalid_author_queries_fail_before_database_io(query: str):
    request = make_mocked_request("GET", "/api/reader/x/authors?" + query)
    response = await authors.handle_x_authors(request)
    assert response.status == 400
