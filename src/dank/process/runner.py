from __future__ import annotations

import asyncio
import datetime
import logging
import re
import time
from collections.abc import Callable
from typing import Any

from dank.config import Settings, load_settings
from dank.embeddings import EmbeddingModel, get_embedding_model
from dank.logging_setup import configure_logging
from dank.model import Asset, Post, RawAsset, RawPost
from dank.process.assets import convert_raw_asset
from dank.process.rss import convert_raw_post as convert_raw_rss_post
from dank.process.x import convert_raw_x_post
from dank.storage.clickhouse import ClickHouseClient, parse_datetime

logger = logging.getLogger(__name__)

RawPostConverter = Callable[[RawPost], Post | None]
RawAssetConverter = Callable[[RawAsset], Asset | None]
MAX_TITLE_EMBED_CHARS = 512
MAX_HTML_EMBED_CHARS = 8192


async def process_source_posts(
    clickhouse_client: ClickHouseClient,
    domain: str,
    converter: RawPostConverter,
    *,
    since: datetime.datetime,
    batch_size: int = 250,
    embedder: EmbeddingModel,
) -> int:
    query = (
        "SELECT "
        "raw.domain AS domain, "
        "raw.post_id AS post_id, "
        "raw.url AS url, "
        "raw.post_created_at AS post_created_at, "
        "raw.scraped_at AS scraped_at, "
        "raw.source AS source, "
        "raw.request_url AS request_url, "
        "raw.payload AS payload "
        "FROM ("
        "SELECT "
        "domain, "
        "post_id, "
        "url, "
        "post_created_at, "
        "scraped_at, "
        "source, "
        "request_url, "
        "payload "
        "FROM raw_posts "
        "WHERE domain = %(domain)s "
        "AND scraped_at >= %(since)s "
        "ORDER BY scraped_at DESC "
        "LIMIT 1 BY post_id"
        ") AS raw "
        "LEFT JOIN ("
        "SELECT domain, post_id, updated_at "
        "FROM posts "
        "WHERE domain = %(domain)s "
        "ORDER BY updated_at DESC "
        "LIMIT 1 BY post_id"
        ") AS processed "
        "ON raw.domain = processed.domain "
        "AND raw.post_id = processed.post_id "
        "WHERE processed.updated_at IS NULL "
        "OR raw.scraped_at > processed.updated_at "
        "ORDER BY raw.scraped_at DESC "
    )
    query_started_at = time.perf_counter()
    result = await clickhouse_client.fetch_json(
        query,
        {"domain": domain, "since": since},
    )
    query_seconds = time.perf_counter() - query_started_at
    logger.info(
        "Loaded %d unprocessed raw posts for domain=%s since=%s in %.3fs",
        len(result.rows),
        domain,
        since.isoformat(),
        query_seconds,
    )

    converted = 0
    batch: list[Post] = []

    for row in result.rows:
        raw = parse_raw_post_row(row)
        post = converter(raw)

        if post is None:
            continue

        batch.append(post)
        converted += 1

        if len(batch) >= batch_size:
            await _insert_posts(clickhouse_client, batch, embedder)
            batch.clear()

    if batch:
        await _insert_posts(clickhouse_client, batch, embedder)

    logger.info(
        "Processed %d posts for domain=%s",
        converted,
        domain,
    )

    return converted


async def process_source_assets(
    clickhouse_client: ClickHouseClient,
    domain: str,
    converter: RawAssetConverter,
    *,
    since: datetime.datetime,
    batch_size: int = 500,
) -> int:
    query = (
        "SELECT "
        "raw.domain AS domain, "
        "raw.post_id AS post_id, "
        "raw.url AS url, "
        "raw.asset_type AS asset_type, "
        "raw.scraped_at AS scraped_at, "
        "raw.source AS source, "
        "raw.local_path AS local_path "
        "FROM ("
        "SELECT "
        "domain, "
        "post_id, "
        "url, "
        "asset_type, "
        "scraped_at, "
        "source, "
        "local_path "
        "FROM raw_assets "
        "WHERE domain = %(domain)s "
        "AND scraped_at >= %(since)s "
        "ORDER BY scraped_at DESC "
        "LIMIT 1 BY post_id, url"
        ") AS raw "
        "LEFT JOIN ("
        "SELECT "
        "domain, "
        "post_id, "
        "url, "
        "updated_at "
        "FROM assets "
        "WHERE domain = %(domain)s "
        "ORDER BY updated_at DESC "
        "LIMIT 1 BY post_id, url"
        ") AS processed "
        "ON raw.domain = processed.domain "
        "AND raw.post_id = processed.post_id "
        "AND raw.url = processed.url "
        "WHERE processed.updated_at IS NULL "
        "OR raw.scraped_at > processed.updated_at "
        "ORDER BY raw.scraped_at DESC"
    )
    query_started_at = time.perf_counter()
    result = await clickhouse_client.fetch_json(
        query,
        {"domain": domain, "since": since},
    )
    query_seconds = time.perf_counter() - query_started_at
    logger.info(
        "Loaded %d unprocessed raw assets for domain=%s since=%s in %.3fs",
        len(result.rows),
        domain,
        since.isoformat(),
        query_seconds,
    )

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
            await clickhouse_client.insert_rows("assets", batch)
            batch.clear()

    if batch:
        await clickhouse_client.insert_rows("assets", batch)

    logger.info(
        "Processed %d assets for domain=%s",
        converted,
        domain,
    )

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


async def _insert_posts(
    clickhouse_client: ClickHouseClient,
    posts: list[Post],
    embedder: EmbeddingModel,
) -> None:
    title_embeddings = await asyncio.to_thread(
        embedder.embed_texts,
        [
            _truncate_for_embedding(post.title, limit=MAX_TITLE_EMBED_CHARS)
            for post in posts
        ],
    )
    html_embeddings = await asyncio.to_thread(
        embedder.embed_texts,
        [
            _truncate_for_embedding(post.html, limit=MAX_HTML_EMBED_CHARS)
            for post in posts
        ],
    )
    posts = [
        post._replace(
            title_embedding=title_embedding,
            html_embedding=html_embedding,
        )
        for post, title_embedding, html_embedding in zip(
            posts,
            title_embeddings,
            html_embeddings,
            strict=True,
        )
    ]

    rows: list[dict[str, Any]] = []

    for post in posts:
        row = post._asdict()
        row["title_embedding"] = list(post.title_embedding)
        row["html_embedding"] = list(post.html_embedding)
        rows.append(row)

    await clickhouse_client.insert_rows(
        "posts",
        rows,
    )


def _truncate_for_embedding(value: str, *, limit: int) -> str:
    return value.strip()[:limit]


async def run_process(
    settings: Settings,
    *,
    age: str = "24h",
) -> int:
    window = parse_age_window(age)
    since = datetime.datetime.now(datetime.UTC) - window
    total_posts = 0
    total_assets = 0
    embedder = get_embedding_model()
    logger.info(
        "Starting process run age=%s since=%s sources=%d",
        age,
        since.isoformat(),
        len(settings.sources),
    )

    async with ClickHouseClient(settings.clickhouse) as clickhouse_client:
        for source in settings.sources:
            converter = (
                convert_raw_x_post
                if source.domain == "x.com"
                else convert_raw_rss_post
            )
            logger.info("Processing source domain=%s", source.domain)

            total_posts += await process_source_posts(
                clickhouse_client,
                source.domain,
                converter,
                since=since,
                embedder=embedder,
            )

            total_assets += await process_source_assets(
                clickhouse_client,
                source.domain,
                convert_raw_asset,
                since=since,
            )

    logger.info(
        "Process run complete posts=%d assets=%d total=%d",
        total_posts,
        total_assets,
        total_posts + total_assets,
    )

    return total_posts + total_assets


def run_process_from_config(
    path: str = "config.toml",
    *,
    age: str = "24h",
) -> int:
    settings = load_settings(path)
    configure_logging(settings.logging, component="process")

    return asyncio.run(run_process(settings, age=age))


def parse_age_window(value: str) -> datetime.timedelta:
    match = re.fullmatch(r"(\d+)\s*([a-z]*)", value.strip().lower())

    if not match:
        raise ValueError("Age must look like 30s, 10m, or 2h")

    amount = int(match.group(1))

    if amount <= 0:
        raise ValueError("Age must be greater than zero")

    # Match the unit.
    match match.group(2):
        case "" | "s" | "sec" | "secs" | "second" | "seconds":
            return datetime.timedelta(seconds=amount)
        case "m" | "min" | "mins" | "minute" | "minutes":
            return datetime.timedelta(minutes=amount)
        case "h" | "hr" | "hrs" | "hour" | "hours":
            return datetime.timedelta(hours=amount)
        case _:
            raise ValueError("Age must be in seconds, minutes, or hours")
