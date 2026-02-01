from __future__ import annotations

import asyncio
import datetime
from collections.abc import Callable
from typing import Any

from dank.embeddings import EmbeddingModel
from dank.model import Asset, Post, RawAsset, RawPost
from dank.storage.clickhouse import ClickHouseClient, parse_datetime

RawPostConverter = Callable[[RawPost], Post | None]
RawAssetConverter = Callable[[RawAsset], Asset | None]


async def process_source_posts(
    client: ClickHouseClient,
    domain: str,
    converter: RawPostConverter,
    *,
    since: datetime.datetime,
    batch_size: int = 100,
    embedder: EmbeddingModel,
) -> int:
    query = (
        "SELECT domain, post_id, url, post_created_at, scraped_at, "
        "source, request_url, payload FROM raw_posts "
        "WHERE domain = %(domain)s "
        "AND coalesce(post_created_at, scraped_at) >= %(since)s "
        "ORDER BY scraped_at DESC "
    )
    result = await client.fetch_json(
        query,
        {"domain": domain, "since": since},
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
            await _insert_posts(client, batch, embedder)
            batch.clear()

    if batch:
        await _insert_posts(client, batch, embedder)

    return converted


async def process_source_assets(
    client: ClickHouseClient,
    domain: str,
    converter: RawAssetConverter,
    *,
    since: datetime.datetime,
    batch_size: int = 100,
) -> int:
    query = (
        "SELECT domain, post_id, url, asset_type, scraped_at, "
        "source, local_path FROM raw_assets "
        "WHERE domain = %(domain)s "
        "AND scraped_at >= %(since)s "
        "ORDER BY scraped_at DESC"
    )
    result = await client.fetch_json(query, {"domain": domain, "since": since})
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
            await client.insert_rows("assets", batch)
            batch.clear()

    if batch:
        await client.insert_rows("assets", batch)

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
    client: ClickHouseClient,
    posts: list[Post],
    embedder: EmbeddingModel,
) -> None:
    # Compute embeddings for all of the posts.
    title_embeddings = await asyncio.to_thread(
        embedder.embed_texts,
        [post.title for post in posts],
    )
    html_embeddings = await asyncio.to_thread(
        embedder.embed_texts,
        [post.html for post in posts],
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

    await client.insert_rows("posts", [post._asdict() for post in posts])
