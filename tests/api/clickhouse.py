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
) -> dict[str, Any]:
    """A processed post; later updated_at values replace earlier ones."""
    return {
        "domain": domain,
        "post_id": post_id,
        "url": url or f"https://{domain}/{post_id}",
        "author": "",
        "title": f"Post {post_id}",
        "html": "",
        "created_at": COLLECTED_AT,
        "updated_at": updated_at,
        "source": source,
    }


def raw_post(
    domain: str,
    post_id: str,
    payload: dict[str, Any],
    *,
    url: str = "",
    source: str = "rss",
) -> dict[str, Any]:
    """A captured payload, as the scrape runner stores it."""
    return {
        "domain": domain,
        "post_id": post_id,
        "url": url or f"https://{domain}/{post_id}",
        "post_created_at": COLLECTED_AT,
        "scraped_at": COLLECTED_AT,
        "source": source,
        "request_url": "",
        "payload": json.dumps(payload),
    }


def x_url(handle: str, post_id: str) -> str:
    return f"https://x.com/{handle}/status/{post_id}"


def reddit_url(community: str, post_id: str) -> str:
    return f"https://www.reddit.com/r/{community}/comments/{post_id}/title/"


async def insert(
    client: ClickHouseClient, table: str, rows: list[dict[str, Any]],
) -> None:
    await client.insert_rows(table, rows)
