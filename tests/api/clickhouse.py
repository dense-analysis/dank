"""Throwaway ClickHouse databases and rows for the clickhouse test lane."""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import re
from typing import Any
from urllib.parse import urlsplit

from dank.config import ClickHouseSettings
from dank.storage.clickhouse import ClickHouseClient

SCHEMA = pathlib.Path(__file__).parents[2] / "schema.sql"
COLLECTED_AT = dt.datetime(2026, 10, 30, 12, tzinfo=dt.UTC)


def server_settings(url: str) -> ClickHouseSettings:
    parts = urlsplit(url)

    return ClickHouseSettings(
        host=parts.hostname or "127.0.0.1",
        port=parts.port or 8123,
        database="default",
        username=parts.username or "default",
        password=parts.password or "",
        secure=parts.scheme == "https",
        use_http=True,
    )


def schema_statements(database: str) -> list[str]:
    # Comments go first, since some contain semicolons. schema.sql names
    # the dank database, so the throwaway database takes its place.
    text = re.sub(r"--[^\n]*", "", SCHEMA.read_text())
    statements = (statement.strip() for statement in text.split(";"))

    return [
        statement.replace("dank.", f"{database}.")
        for statement in statements
        if statement and not statement.startswith("CREATE DATABASE")
    ]


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
