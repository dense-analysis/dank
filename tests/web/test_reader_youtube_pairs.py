from __future__ import annotations

import datetime as dt
from typing import cast
from unittest.mock import AsyncMock

from dank.storage.clickhouse import ClickHouseClient, QueryResult
from dank.web.reader_youtube_pairs import (
    merge_title_pairs,
    thumbnail_title_pairs,
)


async def test_pair_dates_come_from_shared_capture_evidence() -> None:
    first = dt.datetime(2026, 10, 10, tzinfo=dt.UTC)
    last = first + dt.timedelta(days=4)
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([
        {"post_id": "video:abcdefghijk", "content_hash": "image-a",
         "title": "Title A", "first_seen": first, "last_seen": last,
         "reconstructed": 0},
        {"post_id": "video:abcdefghijk", "content_hash": "image-a",
         "title": "Title B", "first_seen": last, "last_seen": last,
         "reconstructed": 1},
    ])
    pairs = await thumbnail_title_pairs(
        cast(ClickHouseClient, client), ["video:abcdefghijk"], ["image-a"],
    )
    saved = pairs[("video:abcdefghijk", "image-a")]
    assert saved[0] == {
        "title": "Title A", "first_seen": first.isoformat(),
        "last_seen": last.isoformat(), "association": "captured",
    }
    assert saved[1]["first_seen"] == last.isoformat()
    assert saved[1]["association"] == "matched"
    query, params = client.fetch_json.call_args.args
    assert "t.captured_url = JSONExtractString" in query
    assert "t.observed_at <= r.finished_at" in query
    assert "uniqExact(d.run_id) = 1" in query
    assert "uniqExact(trimBoth(JSONExtractString" in query
    assert "t.captured_url = t.fetched_url" in query
    assert "title_observed_at <= observed_at" in query
    assert params["limit"] == 20


def test_visual_grouping_keeps_combination_dates_separate() -> None:
    common = {"association": "captured", "first_seen": "2026-10-10",
              "last_seen": "2026-10-11"}
    pairs = merge_title_pairs([
        {**common, "title": "Title A"},
    ], [
        {**common, "title": "Title A", "last_seen": "2026-10-15",
         "association": "matched"},
        {**common, "title": "Title B", "first_seen": "2026-10-14",
         "last_seen": "2026-10-14"},
    ])
    assert pairs[0]["title"] == "Title A"
    assert pairs[0]["first_seen"] == "2026-10-10"
    assert pairs[0]["last_seen"] == "2026-10-15"
    assert pairs[0]["association"] == "matched"
    assert pairs[1]["first_seen"] == "2026-10-14"
    assert pairs[1]["last_seen"] == "2026-10-14"


async def test_no_thumbnail_has_no_title_pair_query() -> None:
    client = AsyncMock()
    assert await thumbnail_title_pairs(client, ["video:abcdefghijk"], []) == {}
    client.fetch_json.assert_not_called()
