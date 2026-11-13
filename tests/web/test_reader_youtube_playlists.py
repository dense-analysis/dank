from __future__ import annotations

import json
from typing import Any

import pytest
from clickhouse_connect.driver.binding import (
    bind_query,  # pyright: ignore[reportUnknownVariableType]
)

from dank.api.youtube.model import (
    PlaylistPage,
    parse_cursor,
    playlist_cursor,
)
from dank.api.youtube.schemas import PlaylistQuery, PlaylistsQuery
from dank.web import reader_youtube_playlists as playlists

STAMP = "2026-10-25T18:00:00.123456+00:00"


def playlist_library(query: dict[str, str]) -> PlaylistPage:
    """The library page GET /api/reader/youtube/playlists reads."""
    return PlaylistsQuery.model_validate(query).library()


def playlist_detail(query: dict[str, str], playlist_id: str) -> PlaylistPage:
    """The page GET /api/reader/youtube/playlists/{id} reads."""
    return PlaylistQuery.model_validate(query).page(playlist_id)


def saved_playlist(**changes: Any) -> dict[str, Any]:
    return {
        "playlist_id": "PLexample",
        "title": "Saved music",
        "sort_key": "0000000001saved music",
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


def test_saved_shorts_playlist_entries_use_short_presentation():
    renderer = {"overlayMetadata": {"primaryText": {"content": "Saved Short"}}}
    row = saved_entry(
        1, payload=json.dumps({"shortsLockupViewModel": renderer}),
    )
    adapted = playlists._entry_row(row, STAMP)  # pyright: ignore[reportPrivateUsage]

    assert adapted["kind"] == "short"
    assert json.loads(adapted["payload"])["shortsLockupViewModel"] == renderer


def test_playlist_cursor_binds_search_target_and_microsecond_snapshot():
    cursor = playlist_cursor(
        "",
        "music",
        STAMP,
        ("saved music", "PLexample"),
    )
    page = playlist_library({"cursor": cursor, "q": "music", "limit": "1"})
    assert page.snapshot == STAMP
    assert page.after == ("saved music", "PLexample")
    sql, params = playlists.library_query(page)
    assert "parseDateTime64BestEffort(%(snapshot)s, 6)" in sql
    assert sql.index("checked_at <=") < sql.index("LIMIT 1 BY playlist_id")
    assert "(sort_key, playlist_id) >" in sql
    assert params["limit"] == 2
    assert params["snapshot"] == STAMP
    bound, _ = bind_query(sql, params)
    assert "%(" not in bound
    count_sql, _ = playlists.library_query(page, count=True)
    assert "after_key" not in count_sql

    with pytest.raises(ValueError):
        playlist_library({"cursor": cursor, "q": "changed"})

    detail = playlist_cursor("PLexample", "", STAMP, 3)
    assert (
        playlist_detail({"cursor": detail}, "PLexample").after
        == 3
    )

    with pytest.raises(ValueError):
        playlist_detail({"cursor": detail}, "PLdifferent")


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
    cursor = playlist_cursor(identity, "", STAMP, after)

    with pytest.raises(ValueError):
        parse_cursor(cursor, identity, "")


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


def test_library_defaults_to_source_order_and_binds_cursor_to_sort():
    recent = playlist_library({})
    assert recent.sort == "recent"
    sql, params = playlists.library_query(recent._replace(snapshot=STAMP))
    assert "youtube_playlist_library" in sql
    assert "observed_at <=" in sql
    assert "indexOf(" in sql
    bound, _ = bind_query(sql, params)
    assert "%(" not in bound
    alphabetical = playlist_library({"sort": "az"})
    sql, _ = playlists.library_query(alphabetical._replace(snapshot=STAMP))
    assert "lowerUTF8(title) AS sort_key" in sql
    assert "indexOf(" not in sql
    cursor = playlist_cursor("", "", STAMP, ("music", "PLid"), "az")
    assert (
        playlist_library({"sort": "az", "cursor": cursor}).sort
        == "az"
    )

    with pytest.raises(ValueError):
        playlist_library({"cursor": cursor})

    with pytest.raises(ValueError):
        playlist_detail({"sort": "az"}, "PLid")


def test_playlist_search_filters_saved_titles_and_channels_before_paging():
    query = "O'Reilly 100%"
    page = playlist_detail({"q": query}, "PLexample")
    assert page.q == query
    sql, params = playlists.playlist_entries_query(
        "PLexample", "snapshot", page,
    )
    assert (
        "concat(title" in sql
        and "channel_name" in sql
        and "channel_url" in sql
    )
    assert sql.index("positionCaseInsensitiveUTF8") < sql.index(
        "ORDER BY position ASC",
    )
    assert params["q"] == query and params["snapshot_hash"] == "snapshot"
    bound, _ = bind_query(sql, params)
    assert "%(" not in bound
    count_sql, count_params = playlists.playlist_entries_query(
        "PLexample",
        "snapshot",
        page._replace(after=50),
        count=True,
    )
    assert "positionCaseInsensitiveUTF8" in count_sql
    assert "position >" not in count_sql and "LIMIT" not in count_sql
    assert count_params["q"] == query
    cursor = playlist_cursor("PLexample", query, STAMP, 77)
    assert (
        playlist_detail({"q": query, "cursor": cursor}, "PLexample").after
        == 77
    )

    for changed in ("", "changed"):
        with pytest.raises(ValueError):
            playlist_detail({"q": changed, "cursor": cursor}, "PLexample")


def test_existing_playlist_rows_expose_saved_views_and_publication_age():
    raw = {
        "videoInfo": {
            "runs": [{"text": "84K"}, {"text": " • "}, {"text": "10h ago"}],
        },
    }
    row = saved_entry(1, payload=json.dumps(raw))
    adapted = playlists._entry_row(row, STAMP)  # pyright: ignore[reportPrivateUsage]
    payload = json.loads(adapted["payload"])
    assert payload["views_text"] == "84K views"
    assert payload["published_text"] == "10h ago"
    assert payload["videoInfo"] == raw["videoInfo"]
