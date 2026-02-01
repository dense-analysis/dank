from __future__ import annotations

import datetime
from collections.abc import Callable
from typing import Any

from dank.model import Post, RawPost
from dank.storage.clickhouse import ClickHouseClient, parse_datetime

RawPostConverter = Callable[[RawPost], Post | None]


async def process_source_posts(
    client: ClickHouseClient,
    source: str,
    converter: RawPostConverter,
    *,
    limit: int = 500,
    batch_size: int = 100,
) -> int:
    query = (
        "SELECT domain, post_id, url, post_created_at, scraped_at, "
        "source, request_url, payload FROM raw_posts "
        f"WHERE source = {quote_literal(source)} "
        "ORDER BY scraped_at DESC "
        f"LIMIT {limit}"
    )
    result = await client.fetch_json(query)
    converted = 0
    batch: list[dict[str, Any]] = []

    for row in result.rows:
        raw = parse_raw_post_row(row)
        post = converter(raw)

        if post is None:
            continue

        batch.append(post._asdict())
        converted += 1

        if len(batch) >= batch_size:
            await client.insert_json_rows("posts", batch)
            batch = []

    if batch:
        await client.insert_json_rows("posts", batch)

    return converted


def parse_raw_post_row(row: dict[str, Any]) -> RawPost:
    scraped_at = parse_datetime(row.get("scraped_at"))

    if scraped_at is None:
        scraped_at = datetime.datetime.now(datetime.UTC)

    return RawPost(
        domain=str(row.get("domain", "")),
        post_id=str(row.get("post_id", "")),
        url=str(row.get("url", "")),
        post_created_at=parse_datetime(row.get("post_created_at")),
        scraped_at=scraped_at,
        source=str(row.get("source", "")),
        request_url=str(row.get("request_url", "")),
        payload=row.get("payload") or "",
    )


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"
