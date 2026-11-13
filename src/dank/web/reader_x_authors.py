"""Author groups over the same saved For You membership as the post feed."""

from __future__ import annotations

import pathlib
from typing import Any

from dank.storage.clickhouse import ClickHouseClient
from dank.web.post_rows import parse_post_row
from dank.web.reader_posts import post_payloads

AUTHOR_EXAMPLES = 3
KNOWN_AUTHOR = "handle != '' AND handle != 'i'"


def author_query(
    base: str, after: tuple[int, str] | None,
) -> tuple[str, dict[str, Any]]:
    # base has one row per saved post, regardless of repeated appearances.
    grouped = (
        "SELECT handle, count() AS count, "
        "argMax(author, (recommended_at, updated_at, post_id)) AS name "
        f"FROM ({base}) WHERE {KNOWN_AUTHOR} GROUP BY handle"
    )
    boundary = ""
    params: dict[str, Any] = {}

    if after:
        boundary = (
            " WHERE count < %(after_count)s OR "
            "(count = %(after_count)s AND handle > %(after_handle)s)"
        )
        params.update(after_count=after[0], after_handle=after[1])

    return (
        f"SELECT * FROM ({grouped}){boundary} "
        "ORDER BY count DESC, handle ASC LIMIT %(limit)s"
    ), params


async def author_examples(
    client: ClickHouseClient, assets_dir: pathlib.Path,
    rows: list[dict[str, Any]], base: str, params: dict[str, Any], search: str,
) -> dict[str, list[dict[str, Any]]]:
    if not rows:
        return {}

    result = await client.fetch_json(
        f"SELECT * FROM ({base}) WHERE handle IN %(author_handles)s "
        "ORDER BY recommended_at DESC, recommendation_position ASC, "
        "domain DESC, post_id DESC LIMIT %(examples)s BY handle",
        {**params, "author_handles": tuple(row["handle"] for row in rows),
         "examples": AUTHOR_EXAMPLES},
    )
    # Use the normal saved-content adapter, including its HTML/media boundary.
    posts = await post_payloads(
        client, [parse_post_row(row) for row in result.rows],
        assets_dir, search,
    )
    handles = {(str(row["domain"]), str(row["post_id"])): str(row["handle"])
               for row in result.rows}
    grouped: dict[str, list[dict[str, Any]]] = {}

    for post in posts:
        handle = handles[str(post["domain"]), str(post["id"])]
        grouped.setdefault(handle, []).append(post)

    return grouped


def author_view(
    row: dict[str, Any], posts: list[dict[str, Any]],
) -> dict[str, Any]:
    identity: dict[str, Any] = posts[0].get("x", {}) if posts else {}

    return {
        "handle": str(row["handle"]),
        "name": identity.get("name") or str(row["name"] or row["handle"]),
        "icon_url": identity.get("icon_url"),
        "count": int(row["count"]), "posts": posts,
    }
