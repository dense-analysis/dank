from __future__ import annotations

import datetime as dt
import json
from typing import Any, cast
from unittest.mock import AsyncMock

import aiohttp
import pytest

from dank.scrape import runner
from dank.scrape.youtube.metadata import (
    MAX_METADATA_BYTES,
    METADATA_FAILURE_TTL,
    METADATA_TTL,
    METADATA_UNAVAILABLE_TTL,
    fetch_video_metadata,
)
from dank.storage.clickhouse import ClickHouseClient, QueryResult

VIDEO = "NnV_cWeoo5Q"
PREVIEW = {
    "title": "Kernel Recipes 2026 - Security in the LLM age",
    "author_name": "Kernel Recipes",
    "author_url": "https://www.youtube.com/@KernelRecipes",
    "type": "video", "version": "1.0",
    "thumbnail_url": f"https://i.ytimg.com/vi/{VIDEO}/hqdefault.jpg",
    "html": '<iframe src="https://www.youtube.com/embed/NnV_cWeoo5Q"></iframe>',
}


class Response:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self.body, self.status = body, status
        self.content = self

    async def __aenter__(self) -> Response:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientError(f"HTTP {self.status}")

    async def iter_chunked(self, size: int):
        for start in range(0, len(self.body), 7):
            yield self.body[start:start + 7]


class HTTP:
    def __init__(self, body: bytes, status: int = 200) -> None:
        self.response = Response(body, status)
        self.urls: list[str] = []

    def get(self, url: str, **kwargs: object) -> Response:
        self.urls.append(url)

        return self.response


async def test_preview_preserves_provider_and_canonical_identity():
    body = json.dumps(PREVIEW, indent=2).encode()
    http = HTTP(body)
    row = await fetch_video_metadata(cast(Any, http), VIDEO)
    payload = json.loads(row["payload"])
    assert row["video_id"] == VIDEO
    assert row["status"] == "captured"
    assert row["provider_payload"] == body.decode()
    assert row["retry_after"] - row["observed_at"] == METADATA_TTL
    assert payload["title"] == PREVIEW["title"]
    assert payload["channel_name"] == "Kernel Recipes"
    assert "channel_id" not in payload
    assert payload["thumbnail_url"] == PREVIEW["thumbnail_url"]
    assert payload["metadata_capture"]["available"] is True
    assert payload["metadata_capture"]["captured_at"]
    assert "discovery" not in payload
    assert "html" not in payload
    assert len(http.urls) == 1
    assert http.urls[0].startswith("https://www.youtube.com/oembed?")


@pytest.mark.parametrize("status", [401, 403, 404, 410])
async def test_unavailable_previews_back_off_and_keep_response(status: int):
    row = await fetch_video_metadata(
        cast(Any, HTTP(b"Unavailable", status)), VIDEO,
    )
    assert row["status"] == "unavailable"
    assert row["http_status"] == status
    assert row["provider_payload"] == "Unavailable"
    assert row["retry_after"] - row["observed_at"] == METADATA_UNAVAILABLE_TTL
    assert "title" not in json.loads(row["payload"])


@pytest.mark.parametrize("body,status", [
    (b"<html>challenge</html>", 200), (b"{}", 200),
    (json.dumps({**PREVIEW, "title": " "}).encode(), 200),
    (b"Rate limited", 429), (b"Busy", 503),
    (b"x" * (MAX_METADATA_BYTES + 1), 200),
])
async def test_invalid_previews_are_failed_attempts(body: bytes, status: int):
    row = await fetch_video_metadata(cast(Any, HTTP(body, status)), VIDEO)
    assert row["status"] == "failed"
    assert row["retry_after"] - row["observed_at"] == METADATA_FAILURE_TTL
    assert "title" not in json.loads(row["payload"])
    assert row["error"]


async def test_untrusted_preview_urls_are_never_projected():
    body = json.dumps({**PREVIEW, "author_url": "javascript:alert(1)",
                       "thumbnail_url": "https://unrelated.example/image"})
    row = await fetch_video_metadata(cast(Any, HTTP(body.encode())), VIDEO)
    payload = json.loads(row["payload"])
    assert "channel_url" not in payload
    assert "thumbnail_url" not in payload


async def test_invalid_video_identity_never_sends_request():
    http = HTTP(b"{}")

    with pytest.raises(ValueError, match="Invalid YouTube"):
        await fetch_video_metadata(cast(Any, http), "../bad")

    assert not http.urls


async def test_network_failure_is_a_retryable_observation():
    http = HTTP(b"{}")

    def fail(*args: Any, **kwargs: Any) -> Response:
        raise aiohttp.ClientError("Connection failed")

    http.get = fail
    row = await fetch_video_metadata(cast(Any, http), VIDEO)
    assert row["status"] == "failed"
    assert row["http_status"] == 0
    assert row["error"] == "Connection failed"


@pytest.mark.parametrize("status", ["captured", "failed", "unavailable"])
async def test_cached_attempts_do_not_fetch_or_append(status: str):
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([{
        "video_id": VIDEO, "status": status,
        "retry_after": dt.datetime.now(dt.UTC) + dt.timedelta(hours=1),
    }])
    http = HTTP(b"{}")
    result = await runner.collect_youtube_video_metadata(
        cast(ClickHouseClient, client), cast(Any, http), VIDEO,
    )
    assert result["cached"] is True
    assert result["status"] == status
    assert not http.urls
    client.insert_rows.assert_not_called()


async def test_due_attempt_appends_without_creating_feed_appearances():
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([{
        "retry_after": dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1),
    }])
    result = await runner.collect_youtube_video_metadata(
        cast(ClickHouseClient, client),
        cast(Any, HTTP(json.dumps(PREVIEW).encode())), VIDEO,
    )
    assert result["status"] == "captured"
    assert result["cached"] is False
    tables = [call.args[0] for call in client.insert_rows.call_args_list]
    assert tables == ["youtube_video_metadata"]


async def test_batch_uses_due_verified_ids_and_bounded_work(
    monkeypatch: pytest.MonkeyPatch,
):
    client = AsyncMock()
    client.fetch_json.return_value = QueryResult([
        {"video_id": VIDEO}, {"video_id": "abcdefghijk"},
    ])
    collector = AsyncMock(return_value={"status": "captured"})
    monkeypatch.setattr(runner, "collect_youtube_video_metadata", collector)
    result = await runner.collect_linked_youtube_metadata(
        cast(ClickHouseClient, client), cast(Any, None),
    )
    query, params = client.fetch_json.call_args.args
    assert "youtube_reference_evidence GROUP BY video_id" in query
    assert "youtube_links" not in query
    assert "m.retry_after <= now64(3)" in query
    assert params["limit"] == 100
    assert len(result) == 2
    assert {call.args[2] for call in collector.call_args_list} == {
        VIDEO, "abcdefghijk",
    }
