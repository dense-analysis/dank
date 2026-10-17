from __future__ import annotations

import datetime as dt
import json
from typing import cast
from unittest.mock import AsyncMock

import pytest

from dank.model import RawPost
from dank.scrape.runner import (
    _flush_posts,  # pyright: ignore[reportPrivateUsage]
)
from dank.storage.clickhouse import ClickHouseClient, QueryResult
from dank.web.reader_youtube import ensure_youtube_storage
from dank.web.reader_youtube_index import backfill_evidence


async def test_backfill_indexes_archived_comments_and_uses_keyset_batches():
    client = AsyncMock()
    timestamp = dt.datetime(2026, 10, 16, tzinfo=dt.UTC)
    client.fetch_json.side_effect = [
        QueryResult([{
            "domain": "reddit.com", "post_id": "t3_thread", "source": "reddit",
            "url": "https://www.reddit.com/comments/thread/",
            "scraped_at": timestamp, "stamp": 123, "fingerprint": 456,
            "payload": json.dumps({"comment_capture": {"comments": [{
                "id": "t1_abc", "parent_id": "t3_thread", "body":
                "A saved talk https://youtu.be/NnV_cWeoo5Q?t=1468",
            }]}}),
        }]), QueryResult([]), QueryResult([]),
    ]
    await backfill_evidence(cast(ClickHouseClient, client))
    table, rows = client.insert_rows.call_args.args
    assert table == "youtube_reference_evidence"
    assert rows[0]["kind"] == "comment"
    assert rows[0]["observed_at"] == timestamp
    assert rows[0]["matched_url"].endswith("?t=1468")
    assert client.fetch_json.call_args_list[1].args[1]["after"] == (
        "reddit.com", "t3_thread", 123, 456,
    )


async def test_failed_backfill_does_not_mark_migration_complete():
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([]), RuntimeError("interrupted"),
    ]

    with pytest.raises(RuntimeError, match="interrupted"):
        await ensure_youtube_storage(cast(ClickHouseClient, client))

    client.insert_rows.assert_not_called()


async def test_new_capture_indexes_evidence_and_keeps_failed_batch_retryable():
    timestamp = dt.datetime(2026, 10, 16, tzinfo=dt.UTC)
    post = RawPost(
        "reddit.com", "t3_thread", "https://www.reddit.com/comments/thread/",
        timestamp, timestamp, "reddit", "https://www.reddit.com/r/example/",
        json.dumps({"url": "https://youtu.be/NnV_cWeoo5Q"}),
    )
    pending = [post]
    client = AsyncMock()
    client.insert_rows.side_effect = [None, RuntimeError("index interrupted")]

    with pytest.raises(RuntimeError, match="index interrupted"):
        await _flush_posts(cast(ClickHouseClient, client), pending)

    assert pending == [post]
    client.insert_rows.side_effect = None
    assert await _flush_posts(cast(ClickHouseClient, client), pending) == 1
    assert pending == []
    table, evidence = client.insert_rows.call_args.args
    assert table == "youtube_reference_evidence"
    assert evidence[0]["video_id"] == "NnV_cWeoo5Q"
    assert evidence[0]["kind"] == "post"


async def test_scoped_backfill_uses_raw_and_processed_batches():
    client = AsyncMock()
    timestamp = dt.datetime(2026, 10, 16, tzinfo=dt.UTC)
    common = {
        "domain": "publisher.example", "post_id": "article",
        "source": "linked_article", "url": "https://publisher.example/article",
        "stamp": 123, "fingerprint": 456,
    }
    client.fetch_json.side_effect = [
        QueryResult([{**common, "scraped_at": timestamp,
            "payload": json.dumps({
                "feed_xml": "<item><title>Publisher</title></item>",
                "page_html": '<article><a href="https://youtu.be/NnV_cWeoo5Q">'
                             'The talk</a></article>',
            }),
        }]), QueryResult([]),
        QueryResult([{**common, "updated_at": timestamp, "author": "Author",
            "title": "Publisher", "html":
            '<p><a href="https://youtu.be/abcdefghijk">Another talk</a></p>',
        }]), QueryResult([]),
    ]
    await backfill_evidence(
        cast(ClickHouseClient, client), sources=("linked_article",),
    )
    evidence = [row for call in client.insert_rows.call_args_list
                for row in call.args[1]]
    assert {row["video_id"] for row in evidence} == {
        "NnV_cWeoo5Q", "abcdefghijk",
    }
    assert {row["source"] for row in evidence} == {"linked_article"}

    for call in client.fetch_json.call_args_list:
        query, params = call.args
        assert "source IN %(sources)s" in query
        assert "linked_article" not in query
        assert params["sources"] == ("linked_article",)


@pytest.mark.parametrize("versions,scoped", [
    ([], False), ([{"version": 1}], False), ([{"version": 2}], True),
])
async def test_index_upgrade_only_scans_newly_supported_sources(
    versions: list[dict[str, int]], *, scoped: bool,
):
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult(versions), QueryResult([]), QueryResult([]),
    ]
    await ensure_youtube_storage(cast(ClickHouseClient, client))
    assert len(client.fetch_json.call_args_list) == 3

    for call in client.fetch_json.call_args_list[1:]:
        query, params = call.args
        assert ("source IN %(sources)s" in query) is scoped
        expected_sources = ("linked_article",) if scoped else ()
        assert params.get("sources", ()) == expected_sources

    table, rows = client.insert_rows.call_args.args
    assert table == "youtube_index_versions"
    assert rows[0]["version"] == 3


async def test_completed_upgrade_does_not_scan_even_when_old_marker_remains():
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([
        {"version": 2}, {"version": 3},
    ])
    await ensure_youtube_storage(cast(ClickHouseClient, client))
    assert client.fetch_json.call_count == 1
    client.insert_rows.assert_not_called()


async def test_failed_scoped_upgrade_does_not_mark_it_complete():
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([{"version": 2}]), RuntimeError("interrupted"),
    ]

    with pytest.raises(RuntimeError, match="interrupted"):
        await ensure_youtube_storage(cast(ClickHouseClient, client))

    client.insert_rows.assert_not_called()
    params = client.fetch_json.call_args.args[1]
    assert params["sources"] == ("linked_article",)
