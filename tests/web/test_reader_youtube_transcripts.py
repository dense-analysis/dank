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

from dank.config import load_settings
from dank.scrape import runner
from dank.storage.clickhouse import QueryResult
from dank.web import reader_youtube_transcripts as api

VIDEO = "L_6pAVHYoFM"


def request(client: Any, method: str = "POST", **kwargs: Any):
    app = web.Application()
    app["clickhouse"] = client
    app["state"] = SimpleNamespace(
        settings=load_settings("config.native.example.toml"),
    )
    api.add_transcript_routes(app)
    headers = {"Host": "127.0.0.1:8080", "Content-Type": "application/json"}
    headers.update(kwargs.get("headers", {}))

    return make_mocked_request(
        method,
        f"/api/reader/youtube/transcript/{VIDEO}",
        app=app,
        match_info={"video_id": kwargs.get("video_id", VIDEO)},
        headers=headers,
    )


@pytest.mark.parametrize(
    "headers,status",
    [
        ({"Origin": "https://unrelated.example"}, 403),
        ({"Sec-Fetch-Site": "cross-site"}, 403),
        ({"Content-Type": "text/plain"}, 415),
    ],
)
async def test_untrusted_requests_cannot_collect(headers: Any, status: int):
    client = AsyncMock()
    response = await api.handle_video_transcript(
        request(client, headers=headers),
    )
    assert response.status == status
    client.fetch_json.assert_not_called()


async def test_invalid_and_unknown_videos_cannot_collect(monkeypatch: Any):
    client = AsyncMock()
    collector = AsyncMock()
    monkeypatch.setattr(api, "collect_youtube_video_transcript", collector)
    response = await api.handle_video_transcript(
        request(client, video_id="bad"),
    )
    assert response.status == 400
    client.fetch_json.assert_not_called()
    client.fetch_json.return_value = QueryResult([])
    response = await api.handle_video_transcript(request(client))
    assert response.status == 404
    collector.assert_not_called()


async def test_read_missing_transcript_never_contacts_provider(
    monkeypatch: Any,
):
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([{"video_id": VIDEO}]),
        QueryResult([]),
    ]
    collector = AsyncMock()
    monkeypatch.setattr(api, "collect_youtube_video_transcript", collector)
    response = await api.handle_video_transcript(request(client, "GET"))
    assert json.loads(response.text or "{}")["status"] == "not_collected"
    collector.assert_not_called()
    client.insert_rows.assert_not_called()


async def test_read_captions_returns_saved_text_without_raw_urls(
    monkeypatch: Any,
):
    now = dt.datetime.now(dt.UTC)
    client = AsyncMock()
    client.fetch_json.side_effect = [
        QueryResult([{"video_id": VIDEO}]),
        QueryResult(
            [
                {
                    "status": "captured",
                    "observed_at": now,
                    "retry_after": now + dt.timedelta(days=1),
                    "track_payload": "private provider URL",
                    "error": "private error",
                    "payload": json.dumps(
                        {
                            "language": "en",
                            "language_name": "English",
                            "generated": True,
                            "segments": [
                                {
                                    "start": 1,
                                    "duration": 2,
                                    "text": "Saved words",
                                },
                            ],
                        },
                    ),
                },
            ],
        ),
    ]
    collector = AsyncMock()
    monkeypatch.setattr(api, "collect_youtube_video_transcript", collector)
    response = await api.handle_video_transcript(request(client, "GET"))
    body = json.loads(response.text or "{}")
    assert body["segments"][0]["text"] == "Saved words"
    assert body["generated"] is True
    assert body["retry_after"] == ""
    assert "private" not in (response.text or "")
    collector.assert_not_called()


async def test_concurrent_requests_reuse_first_capture(monkeypatch: Any):
    client = AsyncMock()
    saved: list[dict[str, Any]] = []

    async def query(sql: str, *args: Any) -> QueryResult:
        return QueryResult(
            [{"video_id": VIDEO}] if "youtube_discoveries" in sql else saved,
        )

    async def insert(table: str, rows: list[dict[str, Any]]):
        saved.extend(rows)

    async def fetch(*args: Any):
        await asyncio.sleep(0.01)

        return {
            "video_id": VIDEO,
            "status": "captured",
            "observed_at": dt.datetime.now(dt.UTC),
            "retry_after": None,
            "payload": "{}",
        }

    client.fetch_json.side_effect = query
    client.insert_rows.side_effect = insert
    collector = AsyncMock(side_effect=fetch)
    monkeypatch.setattr(runner, "fetch_video_transcript", collector)
    req = request(client)
    results = await asyncio.gather(
        api.handle_video_transcript(req),
        api.handle_video_transcript(req),
    )
    assert all(result.status == 200 for result in results)
    assert collector.await_count == 1
    assert len(saved) == 1


async def test_busy_lookup_is_bounded_without_starting_collection(
    monkeypatch: Any,
):
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([{"video_id": VIDEO}])
    collector = AsyncMock()
    monkeypatch.setattr(api, "collect_youtube_video_transcript", collector)
    req = request(client)
    await req.app[api.TRANSCRIPT_LOCK].acquire()

    try:
        response = await api.handle_video_transcript(req)
    finally:
        req.app[api.TRANSCRIPT_LOCK].release()

    assert response.status == 503
    collector.assert_not_called()
