import datetime as dt
import json
from typing import Any, NamedTuple, cast
from unittest.mock import AsyncMock

import pytest

from dank.model import RawPost
from dank.process.reddit import convert_raw_reddit_post, preserve_reddit_body
from dank.process.runner import process_source_posts

NOW = dt.datetime(2026, 10, 16, 20, tzinfo=dt.UTC)


class _Result(NamedTuple):
    rows: list[dict[str, Any]]


class _Embedder:
    def embed_texts(self, texts: list[str]) -> list[tuple[float, ...]]:
        return [() for _ in texts]


def _raw(status: str | None) -> RawPost:
    payload: dict[str, Any] = {
        "title": "Current title",
        "score": 45,
        "selftext": "Truncated",
        "media": [],
        "raw": {"original": "preserved"},
    }

    if status is not None:
        payload["detail_capture"] = {"status": status}

    return RawPost(
        "reddit.com",
        "t3_post",
        "https://www.reddit.com/r/test/comments/post/",
        NOW - dt.timedelta(days=1),
        NOW,
        "reddit",
        "https://www.reddit.com/",
        json.dumps(payload),
    )


def _previous() -> str:
    return json.dumps(
        {
            "title": "Older title",
            "score": 3,
            "detail_capture": {"status": "captured"},
            "selftext_html": "<p>The complete archived post.</p>",
            "media": [
                {"url": "https://i.redd.it/photo.png", "asset_type": "photo"},
            ],
            "is_self": True,
        },
    )


@pytest.mark.parametrize("status", ["failed", "disabled"])
def test_failed_detail_retains_complete_body_without_mutating_raw(
    status: str,
) -> None:
    raw = _raw(status)
    original = raw.payload
    projected = preserve_reddit_body(raw, _previous())
    processed = convert_raw_reddit_post(projected)

    assert processed is not None
    assert processed.title == "Current title"
    assert processed.updated_at == NOW
    assert "The complete archived post." in processed.html
    assert "https://i.redd.it/photo.png" in processed.html
    assert "Truncated" not in processed.html
    assert json.loads(projected.payload)["detail_capture"]["status"] == status
    assert json.loads(projected.payload)["score"] == 45
    assert raw.payload == original


@pytest.mark.parametrize("status", [None, "captured", "unknown"])
def test_successful_or_historical_capture_is_never_replaced(
    status: str | None,
) -> None:
    raw = _raw(status)

    assert preserve_reddit_body(raw, _previous()) == raw


@pytest.mark.parametrize(
    "previous",
    ["bad", "null", "[]", "{}", '{"detail_capture":{"status":"failed"}}'],
)
def test_unusable_history_cannot_replace_body(previous: str) -> None:
    raw = _raw("failed")

    assert preserve_reddit_body(raw, previous) == raw


async def test_processing_recovers_failed_body_from_older_success() -> None:
    raw = _raw("failed")
    client = AsyncMock()
    client.fetch_json.side_effect = [
        _Result([raw._asdict()]),
        _Result([{"post_id": raw.post_id, "payload": _previous()}]),
    ]

    assert (
        await process_source_posts(
            client,
            "reddit.com",
            convert_raw_reddit_post,
            since=NOW - dt.timedelta(hours=1),
            embedder=cast(Any, _Embedder()),
        )
        == 1
    )

    query, params = client.fetch_json.call_args_list[1].args
    assert "domain = 'reddit.com'" in query
    assert "scraped_at < %(before_0)s" in query
    assert (
        "JSONExtractString(payload, 'detail_capture', 'status') = 'captured'"
        in query
    )
    assert params == {"post_0": "t3_post", "before_0": NOW}
    table, rows = client.insert_rows.call_args.args
    assert table == "posts"
    assert "The complete archived post." in rows[0]["html"]
    assert rows[0]["updated_at"] == NOW


@pytest.mark.parametrize("status", [None, "captured"])
async def test_processing_does_not_query_history_for_success_or_old_posts(
    status: str | None,
) -> None:
    client = AsyncMock()
    client.fetch_json.return_value = _Result([_raw(status)._asdict()])

    await process_source_posts(
        client,
        "reddit.com",
        convert_raw_reddit_post,
        since=NOW - dt.timedelta(hours=1),
        embedder=cast(Any, _Embedder()),
    )

    assert client.fetch_json.await_count == 1
