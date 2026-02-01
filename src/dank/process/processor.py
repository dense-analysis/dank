from __future__ import annotations

import datetime
from collections.abc import Callable
from typing import Any

from dank.model import Asset, Post, RawAsset, RawPost
from dank.storage.clickhouse import (
    ClickHouseClient,
    format_datetime,
    parse_datetime,
)

RawPostConverter = Callable[[RawPost], Post | None]
RawAssetConverter = Callable[[RawAsset], Asset | None]


async def process_source_posts(
    client: ClickHouseClient,
    domain: str,
    converter: RawPostConverter,
    *,
    since: datetime.datetime,
    batch_size: int = 100,
) -> int:
    since_literal = _format_datetime_literal(since)
    query = (
        "SELECT domain, post_id, url, post_created_at, scraped_at, "
        "source, request_url, payload FROM raw_posts "
        f"WHERE domain = {quote_literal(domain)} "
        f"AND coalesce(post_created_at, scraped_at) >= {since_literal} "
        "ORDER BY scraped_at DESC "
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
            batch.clear()

    if batch:
        await client.insert_json_rows("posts", batch)

    return converted


async def process_source_assets(
    client: ClickHouseClient,
    domain: str,
    converter: RawAssetConverter,
    *,
    since: datetime.datetime,
    batch_size: int = 100,
) -> int:
    since_literal = _format_datetime_literal(since)
    query = (
        "SELECT domain, post_id, url, asset_type, scraped_at, "
        "source, local_path FROM raw_assets "
        f"WHERE domain = {quote_literal(domain)} "
        f"AND scraped_at >= {since_literal} "
        "ORDER BY scraped_at DESC"
    )
    result = await client.fetch_json(query)
    converted = 0
    batch: list[dict[str, Any]] = []

    for row in result.rows:
        raw = parse_raw_asset_row(row)
        asset = converter(raw)

        if asset is None:
            continue

        batch.append(asset._asdict())
        converted += 1

        if len(batch) >= batch_size:
            await client.insert_json_rows("assets", batch)
            batch.clear()

    if batch:
        await client.insert_json_rows("assets", batch)

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


def parse_raw_asset_row(row: dict[str, Any]) -> RawAsset:
    scraped_at = parse_datetime(row.get("scraped_at"))

    if scraped_at is None:
        scraped_at = datetime.datetime.now(datetime.UTC)

    return RawAsset(
        domain=str(row.get("domain", "")),
        post_id=str(row.get("post_id", "")),
        url=str(row.get("url", "")),
        asset_type=str(row.get("asset_type", "")),
        scraped_at=scraped_at,
        source=str(row.get("source", "")),
        local_path=str(row.get("local_path", "")),
    )


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _format_datetime_literal(value: datetime.datetime) -> str:
    formatted = format_datetime(value)
    if formatted is None:
        formatted = format_datetime(datetime.datetime.now(datetime.UTC))
    return f"toDateTime64('{formatted}', 3, 'UTC')"
