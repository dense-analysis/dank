from __future__ import annotations

import asyncio
import datetime as dt
import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from clickhouse_connect.driver.binding import (
    bind_query,  # pyright: ignore[reportUnknownVariableType]
)

from dank.config import load_settings
from dank.scrape import runner
from dank.scrape.http import current_limiter
from dank.storage.clickhouse import QueryResult
from dank.web import reader_youtube_metadata as metadata
from dank.web.reader_youtube import (
    _item,  # pyright: ignore[reportPrivateUsage]
)
from dank.web.reader_youtube_query import (
    YouTubeFilters,
    build_youtube_query,
    encode_youtube_cursor,
)

VIDEO = "NnV_cWeoo5Q"


def request(client: Any, **kwargs: Any):
    app = web.Application()
    app["clickhouse"] = client
    app["state"] = SimpleNamespace(
        settings=load_settings("config.native.example.toml"),
    )
    metadata.add_metadata_routes(app)
    headers = {"Host": "127.0.0.1:8080", "Content-Type": "application/json"}
    headers.update(kwargs.get("headers", {}))

    return make_mocked_request(
        "POST", f"/api/reader/youtube/metadata/{VIDEO}", app=app,
        match_info={"video_id": kwargs.get("video_id", VIDEO)},
        headers=headers,
    )


@pytest.mark.parametrize("headers,status", [
    ({"Origin": "https://evil.example"}, 403),
    ({"Sec-Fetch-Site": "cross-site"}, 403),
    ({"Content-Type": "application/x-www-form-urlencoded"}, 415),
    ({"Content-Type": "text/plain"}, 415),
])
async def test_collection_rejects_cross_origin_and_form_requests(
    headers: dict[str, str], status: int,
):
    client = AsyncMock()
    response = await metadata.handle_video_metadata(
        request(client, headers=headers),
    )
    assert response.status == status
    client.fetch_json.assert_not_called()


async def test_unknown_or_invalid_video_cannot_start_external_collection():
    client = AsyncMock()
    response = await metadata.handle_video_metadata(
        request(client, video_id="invalid"),
    )
    assert response.status == 400
    client.fetch_json.assert_not_called()
    client.fetch_json.return_value = QueryResult([])
    response = await metadata.handle_video_metadata(request(client))
    assert response.status == 404
    client.insert_rows.assert_not_called()


async def test_cached_collection_returns_complete_capture_state():
    now = dt.datetime.now(dt.UTC)
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([{"video_id": VIDEO}]), QueryResult([{
            "video_id": VIDEO, "status": "captured",
            "provider": "youtube_oembed",
            "observed_at": now, "retry_after": now + dt.timedelta(days=7),
        }]),
    ]
    response = await metadata.handle_video_metadata(request(client))
    body = json.loads(response.text or "{}")
    assert response.status == 200
    assert body["available"] is True
    assert body["captured_at"] == now.isoformat()
    assert body["cached"] is True
    client.insert_rows.assert_not_called()


async def test_failed_refresh_keeps_available_preview_state():
    now = dt.datetime.now(dt.UTC)
    yesterday = now - dt.timedelta(days=1)
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([{"video_id": VIDEO}]), QueryResult([{
            "video_id": VIDEO, "status": "failed",
            "provider": "youtube_oembed",
            "observed_at": now, "retry_after": now + dt.timedelta(hours=1),
        }]), QueryResult([{"observed_at": yesterday}]),
    ]
    response = await metadata.handle_video_metadata(request(client))
    body = json.loads(response.text or "{}")
    assert body["status"] == "failed"
    assert body["available"] is True
    assert body["captured_at"] == yesterday.isoformat()
    assert body["observed_at"] == now.isoformat()


async def test_simultaneous_requests_share_the_persisted_result(
    monkeypatch: pytest.MonkeyPatch,
):
    client = AsyncMock()
    saved: list[dict[str, Any]] = []
    now = dt.datetime.now(dt.UTC)

    async def query(sql: str, params: Any) -> QueryResult:
        if "youtube_reference_evidence" in sql:
            return QueryResult([{"video_id": VIDEO}])

        return QueryResult(saved)

    async def fetch(*args: Any) -> dict[str, Any]:
        await asyncio.sleep(0)

        return {
            "video_id": VIDEO, "status": "captured",
            "provider": "youtube_oembed",
            "observed_at": now, "retry_after": now + dt.timedelta(days=7),
        }

    async def insert(table: str, rows: list[dict[str, Any]]) -> None:
        saved.extend(rows)

    client.fetch_json.side_effect = query
    client.insert_rows.side_effect = insert
    collector = AsyncMock(side_effect=fetch)
    monkeypatch.setattr(runner, "fetch_video_metadata", collector)
    req = request(client)
    responses = await asyncio.gather(
        metadata.handle_video_metadata(req),
        metadata.handle_video_metadata(req),
    )
    assert all(response.status == 200 for response in responses)
    assert collector.await_count == 1
    assert len(saved) == 1
    cached = {json.loads(item.text or "{}")["cached"] for item in responses}
    assert cached == {True, False}


def test_query_enriches_linked_titles_but_preserves_selected_feed_capture():
    linked, _ = build_youtube_query(YouTubeFilters(feed="linked"))
    feed, _ = build_youtube_query(YouTubeFilters(feed="https://www.youtube.com/feed/hype"))
    assert "youtube_video_metadata" in linked
    last_success = "argMaxIf(v.payload, v.observed_at, v.status = 'captured')"
    assert last_success in linked
    assert "if(notEmpty(JSONExtractString(i.payload, 'title'))" not in linked
    assert "if(notEmpty(JSONExtractString(i.payload, 'title'))" in feed
    assert "youtube_video_metadata" in feed


def test_metadata_and_full_details_have_separate_honest_statuses():
    item = _item({"item_id": f"video:{VIDEO}", "payload": json.dumps({
        "title": "Security in the LLM age",
        "metadata_capture": {
            "status": "failed", "available": True,
            "captured_at": "2026-10-10T11:00:00+00:00",
            "observed_at": "2026-10-17T11:00:00+00:00",
            "provider": "youtube_oembed",
        },
    })})
    assert item["title"] == "Security in the LLM age"
    assert item["metadata_capture"]["available"] is True
    assert item["metadata_capture"]["status"] == "failed"
    assert item["detail_capture"]["status"] == "disabled"


async def test_metadata_requests_share_cooldown_but_apps_are_isolated(
    monkeypatch: pytest.MonkeyPatch,
):
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([{"video_id": VIDEO}])
    now = dt.datetime.now(dt.UTC)
    observed: list[object] = []

    async def collect(*args: Any) -> dict[str, Any]:
        observed.append(current_limiter.get())

        return {
            "status": "captured", "provider": "youtube_oembed",
            "observed_at": now, "retry_after": now + dt.timedelta(days=7),
            "cached": False,
        }

    monkeypatch.setattr(metadata, "collect_youtube_video_metadata", collect)
    req = request(client)
    other = request(client)
    await metadata.handle_video_metadata(req)
    await metadata.handle_video_metadata(req)
    await metadata.handle_video_metadata(other)
    assert observed[0] is observed[1] is req.app[metadata.METADATA_LIMITER]
    assert observed[2] is other.app[metadata.METADATA_LIMITER]
    assert observed[0] is not observed[2]
    other_lock = other.app[metadata.METADATA_LOCK]
    assert req.app[metadata.METADATA_LOCK] is not other_lock
    assert current_limiter.get() is None


@pytest.mark.parametrize("feed", ["", "linked", "https://www.youtube.com/feed/hype"])
@pytest.mark.parametrize("count", [False, True])
@pytest.mark.parametrize("paged", [False, True])
@pytest.mark.parametrize("filtered", [False, True])
def test_real_driver_binds_every_metadata_query_shape(
    feed: str, *, count: bool, paged: bool, filtered: bool,
):
    filters = YouTubeFilters(
        feed=feed, item_id=f"video:{VIDEO}" if filtered else "",
        kind="video" if filtered else "",
        channel="https://www.youtube.com/@KernelRecipes" if filtered else "",
        q="Provider's title {subject: security}" if filtered else "",
        limit=17,
    )

    if paged:
        filters = filters._replace(cursor=encode_youtube_cursor(
            filters, "2026-10-17T12:00:00+00:00", f"video:{VIDEO}", 1,
        ))

    query, params = build_youtube_query(filters, count=count)
    bound, external = bind_query(query, params)
    assert not external
    assert "%(" not in bound

    if filtered:
        assert VIDEO in bound
        assert "@KernelRecipes" in bound

    if not count:
        assert "LIMIT 18" in bound
