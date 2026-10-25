from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from urllib.parse import urlencode

import pytest
from aiohttp.test_utils import make_mocked_request
from clickhouse_connect.driver.binding import (
    bind_query,  # pyright: ignore[reportUnknownVariableType]
)
from multidict import MultiDict

from dank.storage.clickhouse import QueryResult
from dank.web import reader_youtube_playlists as playlists

STAMP = "2026-10-25T18:00:00.123456+00:00"


def saved_playlist(**changes: Any) -> dict[str, Any]:
    return {
        "playlist_id": "PLexample",
        "title": "Saved music",
        "sort_title": "saved music",
        "url": "https://www.youtube.com/playlist?list=PLexample",
        "description": "Saved playlist",
        "thumbnail_url": "https://i.ytimg.com/example.jpg",
        "channel_id": "UCexample",
        "channel_name": "Example",
        "channel_url": "",
        "reported_count": 3,
        "entry_count": 3,
        "sort_order": "Manual",
        "snapshot_hash": "old-order",
        "observed_at": STAMP,
        "checked_at": STAMP,
        "status": "complete",
        **changes,
    }


def saved_entry(
    position: int, video_id: str = "aaaaaaaaaaa", **changes: Any,
) -> dict[str, Any]:
    return {
        "position": position,
        "entry_id": f"entry-{position}",
        "video_id": video_id,
        "title": "Saved video",
        "channel_id": "UCexample",
        "channel_name": "Example",
        "channel_url": "",
        "thumbnail_url": "",
        "duration_seconds": 3661,
        "available": True,
        "payload": "{}",
        **changes,
    }


def request_for(
    url: str, client: Any, path: Path, playlist_id: str = "",
) -> Any:
    request = make_mocked_request(
        "GET",
        url,
        match_info={"playlist_id": playlist_id},
    )
    request.app["clickhouse"] = client
    request.app["state"] = SimpleNamespace(assets_dir=path)

    return request


def test_playlist_cursor_binds_search_target_and_microsecond_snapshot():
    cursor = playlists.playlist_cursor(
        "", "music", STAMP, ("saved music", "PLexample"),
    )
    page = playlists.parse_page(
        MultiDict({"cursor": cursor, "q": "music", "limit": "1"}),
    )
    assert page.snapshot == STAMP
    assert page.after == ("saved music", "PLexample")
    sql, params = playlists.library_query(page)
    assert "parseDateTime64BestEffort(%(snapshot)s, 6)" in sql
    assert sql.index("checked_at <=") < sql.index("LIMIT 1 BY playlist_id")
    assert "(lowerUTF8(title), playlist_id) >" in sql
    assert params["limit"] == 2
    assert params["snapshot"] == STAMP
    bound, _ = bind_query(sql, params)
    assert "%(" not in bound
    count_sql, _ = playlists.library_query(page, count=True)
    assert "after_title" not in count_sql

    with pytest.raises(ValueError):
        playlists.parse_page(MultiDict({"cursor": cursor, "q": "changed"}))

    detail = playlists.playlist_cursor("PLexample", "", STAMP, 3)
    assert (
        playlists.parse_page(MultiDict({"cursor": detail}), "PLexample").after
        == 3
    )

    with pytest.raises(ValueError):
        playlists.parse_page(MultiDict({"cursor": detail}), "PLdifferent")


@pytest.mark.parametrize(
    "query",
    [
        "limit=0",
        "limit=101",
        "limit=x",
        "cursor=bad",
        "q=a&q=b",
        "sort=newest",
        "limit=1&limit=2",
    ],
)
async def test_invalid_playlist_pages_fail_before_database(query: str):
    request = make_mocked_request(
        "GET", "/api/reader/youtube/playlists?" + query,
    )
    assert (await playlists.handle_playlists(request)).status == 400


@pytest.mark.parametrize(
    "identity,after",
    [
        ("PLexample", 0),
        ("PLexample", True),
        ("PLexample", "3"),
        ("", ["x"]),
        ("", 2),
    ],
)
def test_invalid_cursor_boundaries_are_rejected(identity: str, after: Any):
    cursor = playlists.playlist_cursor(identity, "", STAMP, after)

    with pytest.raises(ValueError):
        playlists.parse_cursor(cursor, identity, "")


async def test_library_reports_count_status_and_next_cursor(tmp_path: Path):
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult(
            [
                saved_playlist(status="partial"),
                saved_playlist(playlist_id="PLsecond"),
            ],
        ),
        QueryResult([{"total": 2}]),
    ]
    response = await playlists.handle_playlists(
        request_for(
            "/api/reader/youtube/playlists?limit=1",
            client,
            tmp_path,
        ),
    )
    body = json.loads(response.text or "")
    assert body["total"] == 2
    assert body["playlists"][0]["status"] == "partial"
    assert body["playlists"][0]["complete"] is False
    assert body["playlists"][0]["snapshot_id"] == "old-order"
    assert body["next_cursor"]
    snapshot, after = playlists.parse_cursor(body["next_cursor"], "", "")
    assert dt.datetime.fromisoformat(snapshot).tzinfo is not None
    assert after == ("saved music", "PLexample")


async def test_detail_retains_duplicate_occurrences_and_unavailable_positions(
    tmp_path: Path,
):
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([saved_playlist()]),
        QueryResult(
            [
                saved_entry(1),
                saved_entry(2),
                saved_entry(3, "", available=False, title="[Private video]"),
            ],
        ),
        *[QueryResult([]) for _ in range(5)],
    ]
    response = await playlists.handle_playlist(
        request_for(
            "/api/reader/youtube/playlists/PLexample",
            client,
            tmp_path,
            "PLexample",
        ),
    )
    body = json.loads(response.text or "")
    assert body["total"] == 3
    assert [item["id"] for item in body["items"]] == [
        "video:aaaaaaaaaaa",
        "video:aaaaaaaaaaa",
        "unavailable:3",
    ]
    assert [item["playlist_position"] for item in body["items"]] == [1, 2, 3]
    assert [item["playlist_entry_id"] for item in body["items"]] == [
        "entry-1",
        "entry-2",
        "entry-3",
    ]
    assert body["items"][0]["duration_text"] == "1:01:01"
    assert body["items"][2]["available"] is False
    assert body["items"][2]["url"] == ""
    assert body["items"][2]["video_url"] is None
    entries_sql, params = client.fetch_json.call_args_list[1].args
    assert "ORDER BY position ASC" in entries_sql
    assert params["snapshot_hash"] == "old-order"
    assert "GROUP BY" not in entries_sql


async def test_detail_cursor_freezes_metadata_and_entry_revision_after_update(
    tmp_path: Path,
):
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([saved_playlist()]),
        QueryResult([saved_entry(2), saved_entry(3)]),
        *[QueryResult([]) for _ in range(5)],
    ]
    cursor = playlists.playlist_cursor("PLexample", "", STAMP, 1)
    response = await playlists.handle_playlist(
        request_for(
            "/api/reader/youtube/playlists/PLexample?"
            + urlencode({"cursor": cursor, "limit": 1}),
            client,
            tmp_path,
            "PLexample",
        ),
    )
    body = json.loads(response.text or "")
    metadata_sql, metadata_params = client.fetch_json.call_args_list[0].args
    assert "checked_at <=" in metadata_sql
    assert metadata_params["snapshot"] == STAMP
    assert client.fetch_json.call_args_list[1].args[1]["after"] == 1
    assert playlists.parse_cursor(body["next_cursor"], "PLexample", "") == (
        STAMP,
        2,
    )


async def test_missing_playlist_returns_404_without_loading_items(
    tmp_path: Path,
):
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([])
    response = await playlists.handle_playlist(
        request_for(
            "/api/reader/youtube/playlists/missing",
            client,
            tmp_path,
            "missing",
        ),
    )
    assert response.status == 404
    assert client.fetch_json.await_count == 1


def test_playlist_metadata_never_exposes_non_http_artwork_or_owner_urls():
    result = playlists.playlist_metadata(
        saved_playlist(
            thumbnail_url="javascript:alert(1)",
            channel_url="file:///tmp/private",
            reported_count=None,
        ),
    )
    assert result["thumbnail_url"] is None
    assert result["channel"]["url"] == ""
    assert result["video_count"] is None
