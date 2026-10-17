from __future__ import annotations

import asyncio
import datetime
import json
import logging
import pathlib
import re
import time
from collections.abc import Callable
from typing import Any, cast

from dank.config import Settings, SourceConfig, load_settings
from dank.embeddings import EmbeddingModel, get_embedding_model
from dank.html_utils import html_text
from dank.links import LinkPost, normalized_url, post_index_rows
from dank.logging_setup import configure_logging
from dank.model import Asset, Post, RawAsset, RawPost
from dank.process.assets import convert_raw_asset
from dank.process.hacker_news import (
    convert_raw_hacker_news_post,
    preserve_hacker_news_story,
)
from dank.process.reddit import convert_raw_reddit_post, preserve_reddit_body
from dank.process.rss import (
    convert_raw_post as convert_raw_rss_post,
    recover_post_from_page,
)
from dank.process.x import convert_raw_x_post
from dank.process.youtube import convert_raw_youtube_post
from dank.progress import Progress
from dank.storage.clickhouse import ClickHouseClient, parse_datetime
from dank.youtube_references import reference_rows

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
    reprocess: bool = False,
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
        "raw.payload AS payload, "
        "processed.author AS previous_author "
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
        "AND NOT is_reference AND scraped_at >= %(since)s "
        "ORDER BY scraped_at DESC "
        "LIMIT 1 BY post_id"
        ") AS raw "
        "LEFT JOIN ("
        "SELECT domain, post_id, updated_at, author "
        "FROM posts FINAL "
        "WHERE domain = %(domain)s "
        ") AS processed "
        "ON raw.domain = processed.domain "
        "AND raw.post_id = processed.post_id "
        "WHERE %(reprocess)s OR processed.updated_at IS NULL "
        "OR raw.scraped_at > processed.updated_at "
        "ORDER BY raw.scraped_at DESC "
    )
    query_started_at = time.perf_counter()
    result = await clickhouse_client.fetch_json(
        query,
        {"domain": domain, "since": since, "reprocess": reprocess},
    )
    query_seconds = time.perf_counter() - query_started_at
    logger.info(
        "Loaded %d raw posts for domain=%s since=%s in %.3fs",
        len(result.rows),
        domain,
        since.isoformat(),
        query_seconds,
    )

    await _persist_url_aliases(clickhouse_client, result.rows)
    converted = 0
    batch: list[Post] = []
    reddit_bodies = (
        await _previous_reddit_bodies(clickhouse_client, result.rows)
        if domain == "reddit.com" else {}
    )

    hacker_news_stories = (
        await _previous_hacker_news_stories(clickhouse_client, result.rows)
        if domain == "news.ycombinator.com" else {}
    )

    for row in result.rows:
        raw = parse_raw_post_row(row)

        if previous_story := hacker_news_stories.get(raw.post_id):
            raw = preserve_hacker_news_story(raw, previous_story)

        if previous_payload := reddit_bodies.get(raw.post_id):
            raw = preserve_reddit_body(raw, previous_payload)

        post = converter(raw)

        if post is None:
            continue

        # A failed or incomplete newer page must not erase a known byline.
        post = post._replace(
            author=post.author or str(row.get("previous_author") or ""),
        )
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


async def _previous_hacker_news_stories(
    client: ClickHouseClient, rows: list[dict[str, Any]],
) -> dict[str, str]:
    clauses: list[str] = []
    params: dict[str, object] = {}

    for index, row in enumerate(rows):
        raw = parse_raw_post_row(row)

        try:
            payload = json.loads(raw.payload)
        except (TypeError, ValueError):
            continue

        if not isinstance(payload, dict) or "story" in payload:
            continue

        clauses.append(
            f"(post_id = %(post_{index})s "
            f"AND toUnixTimestamp64Milli(scraped_at) < %(before_{index})s)",
        )
        params[f"post_{index}"] = raw.post_id
        params[f"before_{index}"] = round(raw.scraped_at.replace(
            tzinfo=raw.scraped_at.tzinfo or datetime.UTC,
        ).timestamp() * 1000)

    if not clauses:
        return {}

    result = await client.fetch_json(
        "SELECT post_id, payload FROM raw_posts "
        "WHERE domain = 'news.ycombinator.com' AND ("
        + " OR ".join(clauses) + ") "
        "AND JSONHas(payload, 'story') "
        "ORDER BY scraped_at DESC LIMIT 1 BY post_id", params,
    )

    return {str(row["post_id"]): str(row["payload"]) for row in result.rows}


async def _previous_reddit_bodies(
    client: ClickHouseClient, rows: list[dict[str, Any]],
) -> dict[str, str]:
    candidates: list[RawPost] = []

    for row in rows:
        raw = parse_raw_post_row(row)

        try:
            payload = json.loads(raw.payload)
        except (TypeError, ValueError):
            continue

        if not isinstance(payload, dict):
            continue

        payload = cast(dict[str, Any], payload)
        capture = payload.get("detail_capture")

        if isinstance(capture, dict) and cast(dict[str, Any], capture).get(
            "status",
        ) in ("failed", "disabled", "deferred"):
            candidates.append(raw)

    if not candidates:
        return {}

    clauses: list[str] = []
    params: dict[str, object] = {}

    for index, raw in enumerate(candidates):
        clauses.append(
            f"(post_id = %(post_{index})s "
            f"AND toUnixTimestamp64Milli(scraped_at) < %(before_{index})s)",
        )
        params[f"post_{index}"] = raw.post_id
        params[f"before_{index}"] = round(raw.scraped_at.replace(
            tzinfo=raw.scraped_at.tzinfo or datetime.UTC,
        ).timestamp() * 1000)

    result = await client.fetch_json(
        "SELECT post_id, payload FROM raw_posts "
        "WHERE domain = 'reddit.com' AND (" + " OR ".join(clauses) + ") "
        "AND JSONExtractString(payload, 'detail_capture', 'status') "
        "= 'captured' ORDER BY scraped_at DESC LIMIT 1 BY post_id",
        params,
    )

    return {
        str(row["post_id"]): str(row["payload"]) for row in result.rows
    }


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
        is_reference=bool(row.get("is_reference", False)),
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
    posts = await _recover_saved_pages(clickhouse_client, posts)

    async with Progress(f"Embedding and saving {len(posts)} posts"):
        title_embeddings = await asyncio.to_thread(
            embedder.embed_texts,
            [
                _truncate_for_embedding(
                    post.title, limit=MAX_TITLE_EMBED_CHARS,
                )
                for post in posts
            ],
        )
        html_embeddings = await asyncio.to_thread(
            embedder.embed_texts,
            [
                _truncate_for_embedding(
                    html_text(post.html), limit=MAX_HTML_EMBED_CHARS,
                )
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

        identities: list[dict[str, object]] = []
        links: list[dict[str, object]] = []

        for post in posts:
            own, outgoing = post_index_rows(LinkPost(
                post.domain, post.post_id, post.url, post.html, post.source,
                post.updated_at,
            ))
            identities.extend(own)
            links.extend(outgoing)

        # Version-bound links become visible with their processed post.
        await clickhouse_client.insert_rows("post_urls", identities)
        await clickhouse_client.insert_rows("post_links", links)
        await clickhouse_client.insert_rows("posts", rows)
        evidence = [
            link for row in rows for link in reference_rows(row, raw=False)
        ]

        if evidence:
            await clickhouse_client.insert_rows(
                "youtube_reference_evidence", evidence,
            )


async def _persist_url_aliases(
    client: ClickHouseClient, rows: list[dict[str, Any]],
) -> None:
    aliases: list[dict[str, object]] = []

    for row in rows:
        try:
            payload = json.loads(str(row.get("payload") or "{}"))
        except ValueError:
            continue

        if not isinstance(payload, dict):
            continue

        payload = cast(dict[str, Any], payload)
        linked = payload.get("linked_article")
        urls = [str(row.get("url") or ""), payload.get("page_final_url")]

        if isinstance(linked, dict):
            urls.append(cast(dict[str, Any], linked).get("requested_url"))

        for value in urls:
            if isinstance(value, str) and (url := normalized_url(value)):
                aliases.append({
                    "url": url, "domain": str(row["domain"]),
                    "post_id": str(row["post_id"]),
                    "observed_at": parse_datetime(row.get("scraped_at")),
                })

    if aliases:
        await client.insert_rows("post_urls", aliases)


async def _recover_saved_pages(
    client: ClickHouseClient, posts: list[Post],
) -> list[Post]:
    # The latest page can be a challenge even when older captures are usable.
    pending = {
        (post.domain, post.post_id): post for post in posts
        if post.source == "rss" and post.html and "<" not in post.html
    }

    if not pending:
        return posts

    result = await client.fetch_json(
        "SELECT domain, post_id, url, payload FROM raw_posts "
        "WHERE (domain, post_id) IN %(post_keys)s "
        "ORDER BY scraped_at DESC",
        {"post_keys": list(pending)},
    )
    recovered: dict[tuple[str, str], Post] = {}

    for row in result.rows:
        key = (str(row["domain"]), str(row["post_id"]))
        post = pending.get(key)

        if post is None:
            continue

        candidate = recover_post_from_page(
            post, str(row.get("payload") or ""), str(row["url"]),
        )

        if candidate.html != post.html:
            recovered[key] = candidate
            del pending[key]

        if not pending:
            break

    logger.info(
        "Recovered article structure from saved pages: %d", len(recovered),
    )

    return [recovered.get((post.domain, post.post_id), post) for post in posts]


def _truncate_for_embedding(value: str, *, limit: int) -> str:
    return value.strip()[:limit]


async def run_process(
    settings: Settings,
    *,
    age: str = "24h",
    domain_regex: re.Pattern[str] | None = None,
    reprocess: bool = False,
) -> int:
    # DateTime64 supports captures from 1900 onward; keep the query bounded.
    since = (
        datetime.datetime(1900, 1, 1, tzinfo=datetime.UTC)
        if age.strip().lower() == "all"
        else datetime.datetime.now(datetime.UTC) - parse_age_window(age)
    )
    total_posts = 0
    total_assets = 0
    # Collection can configure multiple listings per domain; process it once.
    sources = tuple({
        source.domain: source for source in settings.sources
        if domain_regex is None or domain_regex.search(source.domain)
    }.values())
    embedder = get_embedding_model()
    logger.info(
        "Starting process run age=%s since=%s sources=%d reprocess=%s",
        age,
        since.isoformat(),
        len(sources),
        reprocess,
    )

    async with ClickHouseClient(settings.clickhouse) as clickhouse_client:
        raw_schema = (
            pathlib.Path(__file__).parents[1] / "storage/raw_posts.sql"
        )
        await clickhouse_client.execute(raw_schema.read_text())
        for name in ("links.sql", "youtube.sql"):
            schema = pathlib.Path(__file__).parents[1] / "storage" / name

            for statement in schema.read_text().split(";"):
                if statement.strip():
                    await clickhouse_client.execute(statement)

        sources = await _with_linked_article_sources(
            clickhouse_client, sources, since, domain_regex,
        )
        for source in sources:
            converter = {
                "x.com": convert_raw_x_post,
                "reddit.com": convert_raw_reddit_post,
                "youtube.com": convert_raw_youtube_post,
                "news.ycombinator.com": convert_raw_hacker_news_post,
            }.get(source.domain, convert_raw_rss_post)
            logger.info("Processing source domain=%s", source.domain)

            total_posts += await process_source_posts(
                clickhouse_client,
                source.domain,
                converter,
                since=since,
                embedder=embedder,
                reprocess=reprocess,
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


async def _with_linked_article_sources(
    client: ClickHouseClient, sources: tuple[SourceConfig, ...],
    since: datetime.datetime, domain_regex: re.Pattern[str] | None,
) -> tuple[SourceConfig, ...]:
    result = await client.fetch_json(
        "SELECT DISTINCT domain FROM raw_posts "
        "WHERE source = 'linked_article' AND scraped_at >= %(since)s",
        {"since": since},
    )
    domains = {source.domain for source in sources}
    include_linked = domain_regex is None or "news.ycombinator.com" in domains
    additional = sorted({
        str(row["domain"]) for row in result.rows
        if str(row["domain"]) not in domains and (
            include_linked or (domain_regex is not None
            and domain_regex.search(str(row["domain"])))
        )
    })

    return (*sources, *(SourceConfig(domain, ()) for domain in additional))


def run_process_from_config(
    path: str = "config.toml",
    *,
    age: str = "24h",
    domain_regex: re.Pattern[str] | None = None,
    reprocess: bool = False,
) -> int:
    settings = load_settings(path)
    configure_logging(settings.logging, component="process")

    return asyncio.run(run_process(
        settings, age=age, domain_regex=domain_regex, reprocess=reprocess,
    ))


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
