from __future__ import annotations

import datetime as dt
import json
from typing import Any, cast
from unittest.mock import AsyncMock, Mock
from urllib.parse import urlencode

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from clickhouse_connect.driver.binding import (
    bind_query,  # pyright: ignore[reportUnknownVariableType]
)
from multidict import MultiDict

from dank.model import Post, RawPost
from dank.process.runner import (
    _insert_posts,  # pyright: ignore[reportPrivateUsage]
)
from dank.scrape.runner import (
    _flush_posts,  # pyright: ignore[reportPrivateUsage]
)
from dank.storage.clickhouse import ClickHouseClient, QueryResult
from dank.web.reader_connections import (
    _parameters,  # pyright: ignore[reportPrivateUsage]
    connection_query,
    connections,
    handle_connection,
    handle_connections,
)
from dank.web.reader_reference_index import ensure_reference_storage

STAMP = dt.datetime(2026, 10, 17, 12, microsecond=123000, tzinfo=dt.UTC)
ROOT = {"domain": "origin.test", "id": "123", "url": "https://origin.test/post",
        "title": "The root", "source": "rss"}


def evidence(target: str = "https://target.test/article", **extra: Any,
             ) -> dict[str, Any]:
    return {
        "domain": "origin.test", "post_id": "123", "target_url": target,
        "origin_url": ROOT["url"], "origin_title": ROOT["title"],
        "origin_source": "rss", "kind": "comment", "reference_id": "comment-2",
        "reference_url": ROOT["url"] + "#comment-2", "author": "Reader",
        "matched_url": target + "#exact", "excerpt": "Read the evidence",
        "observed_at": STAMP, "connection_count": 1, **extra,
    }


def client(*responses: list[dict[str, Any]]) -> Any:
    mocked = AsyncMock()
    mocked.fetch_json.side_effect = [QueryResult(rows) for rows in responses]

    return mocked


async def test_connections_preserve_directions_counts_and_uncollected_targets(
):
    mock = client([ROOT], [evidence(connection_count=135)], [evidence()], [])
    data = await connections(
        mock, {"domain": ROOT["domain"], "id": ROOT["id"]},
    )
    assert data is not None
    assert data["incoming_count"] == 135
    assert data["outgoing_count"] == 1
    assert data["limit"] == 100
    assert data["coverage"] == "saved_evidence"
    assert data["incoming"][0]["target"]["post"] == ROOT
    outgoing = data["outgoing"][0]
    assert outgoing["target"] == {
        "url": "https://target.test/article", "post": None,
        "youtube_item_id": None, "title": "https://target.test/article",
    }
    assert outgoing["evidence"]["matched_url"].endswith("#exact")
    assert outgoing["evidence"]["observed_at"] == STAMP.isoformat()
    assert outgoing["direction"] == "outgoing"
    assert data["incoming"][0]["direction"] == "incoming"
    assert mock.fetch_json.call_args.args[1]["origin"] == (
        "origin.test", "123",
    )


async def test_collected_targets_use_alias_identity_and_authoritative_title():
    target = {"matched_target": "https://target.test/article",
              "domain": "target.test", "id": "canonical", "title": "Original",
              "url": "https://target.test/canonical",
              "source": "linked_article"}
    mock = client([ROOT], [], [evidence()], [target])
    data = await connections(
        mock, {"domain": ROOT["domain"], "id": ROOT["id"]},
    )
    assert data is not None
    result = data["outgoing"][0]["target"]
    assert result["post"]["id"] == "canonical"
    assert result["post"]["url"] == target["url"]
    assert result["title"] == "Original"
    query = mock.fetch_json.call_args.args[0]
    assert "u.observed_at <= p.updated_at" in query


async def test_metadata_only_youtube_is_a_real_reader_destination():
    url = "https://www.youtube.com/watch?v=NnV_cWeoo5Q"
    mock = client([ROOT], [], [evidence(url)], [], [
        {"video_id": "NnV_cWeoo5Q", "title": "Security in the LLM age"},
    ])
    data = await connections(
        mock, {"domain": ROOT["domain"], "id": ROOT["id"]},
    )
    assert data is not None
    assert data["outgoing"][0]["target"] == {
        "url": url, "post": None, "youtube_item_id": "video:NnV_cWeoo5Q",
        "title": "Security in the LLM age",
    }
    query = mock.fetch_json.call_args.args[0]
    assert "youtube_video_metadata" in query
    assert "youtube_discoveries" in query
    assert "youtube_reference_evidence" in query


async def test_unknown_root_returns_not_found_without_scanning_evidence():
    mock = client([])
    assert await connections(
        mock, {"domain": "missing", "id": "missing"},
    ) is None
    assert mock.fetch_json.call_count == 1


@pytest.mark.parametrize("query", [
    "", "domain=origin.test", "id=123", "domain=a&id=b&unknown=x",
    "domain=a&domain=b&id=c", "domain=a&id=b&limit=100000",
])
async def test_invalid_queries_are_rejected_without_storage(query: str):
    app = web.Application()
    app["clickhouse"] = AsyncMock()
    response = await handle_connections(make_mocked_request(
        "GET", "/api/reader/connections?" + query, app=app,
    ))
    assert response.status == 400
    app["clickhouse"].fetch_json.assert_not_called()


async def test_exact_historical_lookup_bypasses_neighbourhood_limit():
    app = web.Application()
    mock = client([evidence()], [])
    app["clickhouse"] = mock
    params = {"domain": "origin.test", "id": "123",
              "target": "https://target.test/article#exact", "kind": "comment",
              "reference_id": "comment-2", "observed_at": STAMP.isoformat()}
    response = await handle_connection(make_mocked_request(
        "GET", "/api/reader/connection?" + urlencode(params), app=app,
    ))
    assert response.status == 200
    payload = json.loads(response.text or "")
    assert payload["evidence"]["observed_at"] == STAMP.isoformat()
    query, bound = mock.fetch_json.call_args_list[0].args
    assert "parseDateTime64BestEffort(%(observed_at)s, 3)" in query
    assert "e.reference_id = %(reference_id)s" in query
    assert bound["target"] == "https://target.test/article"
    assert bound["observed_at"] == STAMP.isoformat()
    assert bound["limit"] == 1


@pytest.mark.parametrize("replacement", [
    {"target": "javascript:alert(1)"}, {"kind": "quote"},
    {"reference_id": ""}, {"observed_at": "not a date"},
    {"kind": "post"},
])
def test_historical_lookup_validates_identity(replacement: dict[str, str]):
    query = MultiDict({"domain": "origin.test", "id": "123",
                       "target": "https://target.test", "kind": "comment",
                       "reference_id": "c1", **replacement})
    with pytest.raises(ValueError):
        _parameters(query, single=True)


@pytest.mark.parametrize("incoming,single", [(True, False), (False, False),
                                               (False, True)])
def test_sql_binds_real_driver_and_limits_after_deduplicating(
    *, incoming: bool, single: bool,
):
    query = connection_query(incoming=incoming, single=single, observed=single)
    bound, bind_params = bind_query(query, {
        "domain": "origin.test'quoted", "id": "123", "target": "https://t.test",
        "kind": "comment", "reference_id": "c1",
        "observed_at": STAMP.isoformat(),
        "limit": 100,
    })
    assert "%(" not in bound
    assert not bind_params

    if single:
        assert "2026-10-17T12:00:00.123000+00:00" in bound
    assert "origin.test\\'quoted" in bound
    assert "e.observed_at <= origin.updated_at" in query
    assert query.index("LIMIT 1 BY") < query.index("LIMIT %(limit)s")
    assert "count() OVER ()" in query
    assert "INNER JOIN" in query and "FROM posts FINAL" in query


async def test_versioned_backfill_is_bounded_and_only_marks_success():
    row = {"source": "hacker_news", "domain": "news.ycombinator.com",
           "post_id": "123", "url": "https://news.ycombinator.com/item?id=123",
           "scraped_at": STAMP, "stamp": 123, "fingerprint": 456,
           "payload": json.dumps({"story": {"url": "https://target.test"}})}
    mock = client([], [row], [], [])
    await ensure_reference_storage(cast(ClickHouseClient, mock))
    assert mock.fetch_json.call_args_list[2].args[1] == {
        "limit": 64, "after": ("news.ycombinator.com", 123, "123", 456),
    }
    query, params = mock.fetch_json.call_args_list[2].args
    bound, _ = bind_query(query, params)
    assert "toUnixTimestamp64Milli(scraped_at)" in bound
    assert "('news.ycombinator.com', 123, '123', 456)" in bound
    first = mock.insert_rows.call_args_list[0].args
    assert first[0] == "post_reference_evidence"
    assert first[1][0]["matched_url"] == "https://target.test"
    assert mock.insert_rows.call_args.args[0] == "reference_index_versions"
    mock = client([])
    mock.fetch_json.side_effect = [
        QueryResult([]), RuntimeError("interrupted"),
    ]
    with pytest.raises(RuntimeError, match="interrupted"):
        await ensure_reference_storage(cast(ClickHouseClient, mock))
    mock.insert_rows.assert_not_called()


async def test_completed_reference_migration_does_not_rescan_archive():
    mock = client([{"version": 1}])
    await ensure_reference_storage(cast(ClickHouseClient, mock))
    assert mock.fetch_json.call_count == 1
    assert mock.execute.call_count == 2
    mock.insert_rows.assert_not_called()


async def test_raw_evidence_write_failure_keeps_capture_retryable():
    post = RawPost(
        "news.ycombinator.com", "123", "https://news.ycombinator.com/item?id=123",
        STAMP, STAMP, "hacker_news", "https://news.ycombinator.com",
        json.dumps({"story": {"url": "https://target.test/paper"}}),
    )
    mock = AsyncMock()
    mock.insert_rows.side_effect = [None, RuntimeError("index interrupted")]
    pending = [post]

    with pytest.raises(RuntimeError, match="index interrupted"):
        await _flush_posts(mock, pending)

    assert pending == [post]
    mock.insert_rows.side_effect = None
    await _flush_posts(mock, pending)
    assert not pending
    assert mock.insert_rows.call_args.args[0] == "post_reference_evidence"


async def test_processed_evidence_is_persisted_before_publishing_the_post():
    post = Post(
        "origin.test", "123", ROOT["url"], STAMP, STAMP, "Writer", "Article",
        (), '<p>See <a href="https://target.test/paper">the paper</a></p>', (),
        "rss",
    )
    mock = AsyncMock()
    await _insert_posts(
        mock, [post], Mock(embed_texts=Mock(return_value=[()])),
    )
    tables = [call.args[0] for call in mock.insert_rows.call_args_list]
    assert tables.index("post_reference_evidence") < tables.index("posts")


async def test_backfill_reuses_identical_content_but_preserves_capture_dates(
    monkeypatch: pytest.MonkeyPatch,
):
    from dank.web import reader_reference_index

    later = STAMP + dt.timedelta(milliseconds=500)
    common = {"source": "hacker_news", "domain": "news.ycombinator.com",
              "post_id": "123", "url": "https://news.ycombinator.com/item?id=123",
              "fingerprint": 456, "stamp": 123,
              "payload": json.dumps({"story": {"url": "https://target.test"}})}
    first = {**common, "scraped_at": STAMP}
    second = {**common, "scraped_at": later, "stamp": 623}
    extract = Mock(wraps=reader_reference_index.reference_rows)
    monkeypatch.setattr(reader_reference_index, "reference_rows", extract)
    mock = client([], [first], [second], [], [])
    await ensure_reference_storage(mock)
    observations = [row["observed_at"]
                    for call in mock.insert_rows.call_args_list
                    if call.args[0] == "post_reference_evidence"
                    for row in call.args[1]]
    assert observations == [STAMP, later]
    assert extract.call_count == 1
