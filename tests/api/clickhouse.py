"""Synthetic ClickHouse rows and a replay client for adapter tests."""

from __future__ import annotations

import datetime as dt
import json
import pathlib
from collections import deque
from typing import Any, cast

from dank.storage.clickhouse import ClickHouseClient, QueryResult

COLLECTED_AT = dt.datetime(2026, 10, 30, 12, tzinfo=dt.UTC)


class QueryResults:
    def __init__(self, responses: tuple[list[dict[str, Any]], ...]) -> None:
        self.responses = deque(responses)
        self.requests: list[tuple[str, dict[str, Any] | None]] = []
        self.writes: list[tuple[str, list[dict[str, Any]]]] = []

    async def fetch_json(
        self, query: str, params: dict[str, Any] | None = None,
    ) -> QueryResult:
        assert self.responses, f"Unexpected ClickHouse statement: {query}"
        self.requests.append((query, params))

        return QueryResult(self.responses.popleft())

    async def insert_rows(
        self, table: str, rows: list[dict[str, Any]],
    ) -> None:
        self.writes.append((table, rows))


def query_client(*responses: list[dict[str, Any]]) -> ClickHouseClient:
    return cast(ClickHouseClient, QueryResults(responses))


def post(
    domain: str,
    post_id: str,
    *,
    url: str = "",
    source: str = "rss",
    updated_at: dt.datetime = COLLECTED_AT,
    created_at: dt.datetime = COLLECTED_AT,
    author: str = "",
    title: str = "",
    html: str = "",
    embedding: tuple[float, ...] = (),
) -> dict[str, Any]:
    """A processed post; later updated_at values replace earlier ones."""
    return {
        "domain": domain,
        "post_id": post_id,
        "url": url or f"https://{domain}/{post_id}",
        "author": author,
        "title": title or f"Post {post_id}",
        "html": html,
        "title_embedding": list(embedding),
        "html_embedding": list(embedding),
        "created_at": created_at,
        "updated_at": updated_at,
        "source": source,
    }


def raw_post(
    domain: str,
    post_id: str,
    payload: dict[str, Any] | str,
    *,
    url: str = "",
    source: str = "rss",
    scraped_at: dt.datetime = COLLECTED_AT,
) -> dict[str, Any]:
    """A captured payload, as the scrape runner stores it.

    It describes the post whose updated_at equals its scraped_at. Text goes
    in as it is, for payloads that aren't valid JSON.
    """
    return {
        "domain": domain,
        "post_id": post_id,
        "url": url or f"https://{domain}/{post_id}",
        "post_created_at": COLLECTED_AT,
        "scraped_at": scraped_at,
        "source": source,
        "request_url": "",
        "payload": (
            payload if isinstance(payload, str) else json.dumps(payload)
        ),
    }


def asset(
    domain: str,
    post_id: str,
    url: str,
    local_path: pathlib.Path,
    content_type: str = "image/jpeg",
) -> dict[str, Any]:
    return {
        "domain": domain,
        "post_id": post_id,
        "url": url,
        "local_path": str(local_path),
        "content_type": content_type,
        "size_bytes": 0,
        "created_at": COLLECTED_AT,
        "updated_at": COLLECTED_AT,
        "source": "rss",
    }


def x_url(handle: str, post_id: str) -> str:
    return f"https://x.com/{handle}/status/{post_id}"


def reddit_url(community: str, post_id: str) -> str:
    return f"https://www.reddit.com/r/{community}/comments/{post_id}/title/"


async def insert(
    client: ClickHouseClient, table: str, rows: list[dict[str, Any]],
) -> None:
    await client.insert_rows(table, rows)
