"""Author groups over the same saved For You membership as the post feed."""

from __future__ import annotations

import base64
import datetime as dt
import json
import re
from typing import Any, cast

from aiohttp import web
from multidict import MultiDict

from dank.storage.clickhouse import ClickHouseClient
from dank.web.app import (
    _parse_post_row,  # pyright: ignore[reportPrivateUsage]
    _search_embedding,  # pyright: ignore[reportPrivateUsage]
)
from dank.web.reader_accounts import X_ACCOUNT_SQL
from dank.web.reader_query import ReaderFilters, build_query, parse_filters

AUTHOR_PAGE_LIMIT = 50
AUTHOR_EXAMPLES = 3
KNOWN_AUTHOR = "handle != '' AND handle != 'i'"


def author_cursor(
    filters: ReaderFilters, snapshot: str, count: int, handle: str,
) -> str:
    return base64.urlsafe_b64encode(json.dumps([
        "x-authors", filters.signature(), snapshot, count, handle,
    ]).encode()).decode()


def parse_author_cursor(
    filters: ReaderFilters, cursor: str,
) -> tuple[str, int, str]:
    try:
        if len(cursor) > 8192:
            raise ValueError

        decoded: object = json.loads(base64.b64decode(
            cursor, altchars=b"-_", validate=True,
        ))

        if not isinstance(decoded, list):
            raise ValueError

        raw = cast(list[Any], decoded)

        if (len(raw) != 5 or raw[:2] != ["x-authors", filters.signature()]
                or not isinstance(raw[2], str)
                or type(raw[3]) is not int or not 1 <= raw[3] < 2**53
                or not isinstance(raw[4], str)
                or not re.fullmatch(r"[a-z0-9_]{1,15}", raw[4])):
            raise ValueError

        snapshot = dt.datetime.fromisoformat(raw[2])

        if snapshot.tzinfo is None:
            raise ValueError

        return snapshot.astimezone(dt.UTC).isoformat(), raw[3], raw[4]
    except (ValueError, TypeError, UnicodeError):
        raise ValueError("Invalid author cursor for these filters") from None


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


async def handle_x_authors(request: web.Request) -> web.Response:
    try:
        query = MultiDict(request.query)

        if len(query.getall("cursor", [])) > 1:
            raise ValueError("Repeated author cursor")

        cursor = query.pop("cursor", "")
        query.setdefault("limit", "20")
        filters = parse_filters(query)

        if filters.x_feed != "for_you" or filters.limit > AUTHOR_PAGE_LIMIT:
            raise ValueError("Authors require X For You and a limit up to 50")

        snapshot = dt.datetime.now(dt.UTC).isoformat()
        after = None

        if cursor:
            snapshot, count, handle = parse_author_cursor(filters, cursor)
            after = (count, handle)
    except ValueError as error:
        return web.json_response({"error": str(error)}, status=400)

    client: ClickHouseClient = request.app["clickhouse"]
    state = request.app["state"]
    embedding = None

    if filters.q and filters.mode in {"meaning", "combined"}:
        embedding = await _search_embedding(client, search_text=filters.q)

    base, params = build_query(
        filters, state.settings.sources, embedding,
        paginate=False, snapshot_time=snapshot,
    )
    base = f"SELECT *, {X_ACCOUNT_SQL} AS handle FROM ({base})"
    sql, boundary = author_query(base, after)
    params.update(boundary)
    result = await client.fetch_json(sql, params)
    rows = result.rows[:filters.limit]
    totals = await client.fetch_json(
        f"SELECT uniqExactIf(handle, {KNOWN_AUTHOR}) AS total, "
        f"countIf(NOT ({KNOWN_AUTHOR})) AS unattributed_posts FROM ({base})",
        params,
    )
    examples = await author_examples(request, rows, base, params, filters.q)
    authors = [_author(row, examples.get(str(row["handle"]), []))
               for row in rows]
    next_cursor = (
        author_cursor(filters, snapshot, int(rows[-1]["count"]),
                      str(rows[-1]["handle"]))
        if len(result.rows) > filters.limit and rows else None
    )
    total = totals.rows[0] if totals.rows else {}

    return web.json_response({
        "authors": authors, "next_cursor": next_cursor,
        "total": int(total.get("total", 0)),
        "unattributed_posts": int(total.get("unattributed_posts", 0)),
    })


async def author_examples(
    request: web.Request, rows: list[dict[str, Any]], base: str,
    params: dict[str, Any], search: str,
) -> dict[str, list[dict[str, Any]]]:
    if not rows:
        return {}

    # Use the normal saved-content adapter, including its HTML/media boundary.
    from dank.web.reader_api import (
        _post_payloads,  # pyright: ignore[reportPrivateUsage]
    )

    client: ClickHouseClient = request.app["clickhouse"]
    result = await client.fetch_json(
        f"SELECT * FROM ({base}) WHERE handle IN %(author_handles)s "
        "ORDER BY recommended_at DESC, recommendation_position ASC, "
        "domain DESC, post_id DESC LIMIT %(examples)s BY handle",
        {**params, "author_handles": tuple(row["handle"] for row in rows),
         "examples": AUTHOR_EXAMPLES},
    )
    posts = await _post_payloads(
        client, [_parse_post_row(row) for row in result.rows],
        request.app["state"].assets_dir, search,
    )
    handles = {(str(row["domain"]), str(row["post_id"])): str(row["handle"])
               for row in result.rows}
    grouped: dict[str, list[dict[str, Any]]] = {}

    for post in posts:
        handle = handles[str(post["domain"]), str(post["id"])]
        grouped.setdefault(handle, []).append(post)

    return grouped


def _author(
    row: dict[str, Any], posts: list[dict[str, Any]],
) -> dict[str, Any]:
    identity: dict[str, Any] = posts[0].get("x", {}) if posts else {}

    return {
        "handle": str(row["handle"]),
        "name": identity.get("name") or str(row["name"] or row["handle"]),
        "icon_url": identity.get("icon_url"),
        "count": int(row["count"]), "posts": posts,
    }
