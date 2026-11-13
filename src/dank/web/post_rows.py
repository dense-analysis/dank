"""Posts and assets as ClickHouse returns them, parsed into rows."""

from __future__ import annotations

import datetime
from typing import Any, cast

from dank.api.posts.model import AssetRow, PostRow
from dank.storage.clickhouse import ClickHouseClient, parse_datetime


async def fetch_post(
    clickhouse_client: ClickHouseClient,
    *,
    domain: str,
    post_id: str,
) -> PostRow | None:
    query = (
        "SELECT domain, post_id, url, author, title, html, created_at, "
        "updated_at, source, categories FROM posts FINAL "
        "WHERE domain = %(domain)s "
        "AND post_id = %(post_id)s "
    )
    result = await clickhouse_client.fetch_json(
        query,
        {"domain": domain, "post_id": post_id},
    )

    if not result.rows:
        return None

    return parse_post_row(result.rows[0])


def parse_post_row(row: dict[str, Any]) -> PostRow:
    created_at = parse_datetime(row.get("created_at"))
    updated_at = parse_datetime(row.get("updated_at"))

    if created_at is None:
        created_at = datetime.datetime.now(datetime.UTC)

    if updated_at is None:
        updated_at = created_at

    return PostRow(
        domain=str(row.get("domain", "")),
        post_id=str(row.get("post_id", "")),
        url=str(row.get("url", "")),
        author=str(row.get("author", "")),
        title=str(row.get("title", "")),
        html=str(row.get("html", "")),
        created_at=created_at,
        updated_at=updated_at,
        source=str(row.get("source", "")),
        categories=tuple(
            (str(cast(dict[str, Any], item)["name"]),
             float(cast(dict[str, Any], item)["confidence"]),
             float(cast(dict[str, Any], item)["similarity"]))
            if isinstance(item, dict)
            else (str(item[0]), float(item[1]), float(item[2]))
            for item in cast(list[Any], row.get("categories", []))
        ),
    )


def parse_asset_row(row: dict[str, Any]) -> AssetRow:
    size = row.get("size_bytes")

    if not isinstance(size, int):
        size = 0

    return AssetRow(
        post_id=str(row.get("post_id", "")),
        url=str(row.get("url", "")),
        local_path=str(row.get("local_path", "")),
        content_type=str(row.get("content_type", "")),
        size_bytes=size,
    )
