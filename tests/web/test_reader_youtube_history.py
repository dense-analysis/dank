from __future__ import annotations

import base64
import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import urlencode

import pytest
from aiohttp.test_utils import make_mocked_request

from dank.storage.clickhouse import QueryResult
from dank.web import reader_youtube as reader
from dank.web.reader_youtube_query import (
    YouTubeFilters,
    parse_youtube_filters,
    youtube_cursor_snapshot,
)

HOME = "https://www.youtube.com/"
ANCHOR = "2026-10-23T12:00:00+00:00"


def row(identifier: str, hour: int) -> dict[str, object]:
    return {
        "item_id": "video:" + identifier,
        "observed_at": f"2026-10-23T{hour:02d}:00:00+00:00",
        "position": 1, "reference_count": 0,
    }


async def test_home_pages_and_totals_reuse_one_persisted_capture_cutoff(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    first, second, third = (row("aaaaaaaaaaa", 11), row("bbbbbbbbbbb", 9),
                            row("ccccccccccc", 8))
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([{"snapshot_time": dt.datetime.fromisoformat(ANCHOR)}]),
        QueryResult([first, second]), QueryResult([{"total": 3}]),
        QueryResult([second, third]), QueryResult([{"total": 3}]),
        QueryResult([third]), QueryResult([{"total": 3}]),
    ]
    monkeypatch.setattr(reader, "youtube_items", AsyncMock(side_effect=[
        [{"id": entry["item_id"], "observed_at": entry["observed_at"]}]
        for entry in (first, second, third)
    ]))
    cursor = ""
    items: list[str] = []

    for _ in range(3):
        values = {"feed": HOME, "limit": "1"}

        if cursor:
            values["cursor"] = cursor

        request = make_mocked_request(
            "GET", "/api/reader/youtube?" + urlencode(values),
        )
        request.app["clickhouse"] = client
        request.app["state"] = SimpleNamespace(assets_dir=tmp_path)
        response = await reader.handle_youtube(request)
        assert response.status == 200
        body = json.loads(response.text or "")
        assert body["total"] == 3
        items.extend(item["id"] for item in body["items"])
        cursor = body["next_cursor"]

        if cursor:
            filters = parse_youtube_filters(request.query)._replace(
                cursor=cursor,
            )
            assert youtube_cursor_snapshot(filters) == ANCHOR

    assert cursor is None
    assert len(set(items)) == 3
    calls = client.fetch_json.call_args_list
    snapshot_calls = [call for call in calls
                      if "AS snapshot_time" in call.args[0]]
    assert len(snapshot_calls) == 1
    assert all(call.args[1]["snapshot_time"] == ANCHOR for call in calls[1:])


def test_other_feed_cursors_do_not_change_shape():
    from dank.web.reader_youtube_query import encode_youtube_cursor

    filters = YouTubeFilters(feed="https://www.youtube.com/feed/hype")
    cursor = encode_youtube_cursor(filters, ANCHOR, "video:aaaaaaaaaaa")
    assert len(json.loads(base64.urlsafe_b64decode(cursor))) == 5
