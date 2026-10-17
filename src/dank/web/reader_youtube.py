"""Feed-first YouTube collection and cross-source video references."""

from __future__ import annotations

import datetime as dt
import pathlib
from typing import Any
from urllib.parse import urlencode, urlsplit

from aiohttp import web

from dank.storage.clickhouse import ClickHouseClient, parse_datetime
from dank.web.app import (
    _asset_view,  # pyright: ignore[reportPrivateUsage]
    _parse_asset_row,  # pyright: ignore[reportPrivateUsage]
)
from dank.web.reader_youtube_query import (
    YouTubeFilters,
    build_youtube_query,
    encode_youtube_cursor,
    parse_youtube_filters,
)
from dank.web.reader_youtube_sources import configured_sources
from dank.youtube import VIDEO_ID, payload_object

MAX_ITEM_DISCOVERIES = 100
MAX_ITEM_REFERENCES = 100
INDEX_VERSION = 1


async def ensure_youtube_storage(client: ClickHouseClient) -> None:
    schema = pathlib.Path(__file__).parents[1] / "storage/youtube.sql"
    statements = [
        s.strip() for s in schema.read_text().split(";") if s.strip()
    ]

    for statement in statements:
        await client.execute(statement)

    complete = await client.fetch_json(
        "SELECT version FROM youtube_index_versions "
        "WHERE version = %(version)s "
        "LIMIT 1", {"version": INDEX_VERSION},
    )

    if complete.rows:
        return

    # Views cover concurrent/new inserts. This one-time backfill is idempotent:
    # repeated source observations collapse to the same reference identity.
    for statement in statements:
        if statement.startswith("CREATE MATERIALIZED VIEW"):
            select = statement.split("TO youtube_links AS", 1)[1]
            await client.execute("INSERT INTO youtube_links " + select)

    await client.insert_rows("youtube_index_versions", [{
        "version": INDEX_VERSION, "completed_at": dt.datetime.now(dt.UTC),
    }])


def add_youtube_routes(app: web.Application) -> None:
    app.router.add_get("/api/reader/youtube", handle_youtube)
    app.router.add_get("/api/reader/youtube/sources", handle_youtube_sources)


async def handle_youtube(request: web.Request) -> web.Response:
    try:
        filters = parse_youtube_filters(request.query)
    except ValueError as error:
        return web.json_response({"error": str(error)}, status=400)

    client: ClickHouseClient = request.app["clickhouse"]
    query, params = build_youtube_query(filters)
    result = await client.fetch_json(query, params)
    rows = result.rows[:filters.limit]
    query, params = build_youtube_query(filters, count=True)
    counted = await client.fetch_json(query, params)
    total = int(counted.rows[0]["total"]) if counted.rows else 0
    state = request.app["state"]
    items = await youtube_items(client, rows, filters, state.assets_dir)
    cursor = None

    if len(result.rows) > filters.limit and items:
        last = items[-1]
        cursor = encode_youtube_cursor(
            filters, last["observed_at"], last["id"],
            int(rows[-1].get("position") or 0),
        )

    return web.json_response({
        "items": items, "next_cursor": cursor, "total": total,
    })


async def youtube_items(
    client: ClickHouseClient, rows: list[dict[str, Any]],
    filters: YouTubeFilters, assets_dir: pathlib.Path | None = None,
) -> list[dict[str, Any]]:
    if not rows:
        return []

    ids = [str(row["item_id"]) for row in rows]
    discoveries = await _discoveries(client, ids, filters)
    references, counts = await _references(client, ids)
    media = await _local_media(client, ids, assets_dir) if assets_dir else {}
    items: list[dict[str, Any]] = []

    for row in rows:
        item_id = str(row["item_id"])
        item = _item(row)
        item.update(media.get(item_id, {}))
        item.update(
            discoveries=discoveries.get(item_id, []),
            references=references.get(item_id, []),
            reference_count=counts.get(item_id, 0),
        )

        if not item["title"]:
            refs = references.get(item_id, [])
            item["title"] = next(
                (r["title"] for r in refs if r["title"]),
                "Linked YouTube video",
            )

        items.append(item)

    return items


def _item(row: dict[str, Any]) -> dict[str, Any]:
    payload = payload_object(row.get("payload"))
    item_id = str(row["item_id"])
    is_video = item_id.startswith("video:")
    video_id = item_id.removeprefix("video:") if is_video else None
    capture = payload_object(payload.get("detail_capture"))
    status = capture.get("status")

    return {
        "id": item_id, "kind": str(row.get("kind") or "video"),
        "video_id": video_id,
        "url": (
            f"https://www.youtube.com/watch?v={video_id}" if is_video else
            f"https://www.youtube.com/post/{item_id.removeprefix('post:')}"
        ),
        "title": str(payload.get("title") or ""),
        "text": str(payload.get("text") or ""),
        "channel": {
            "id": str(payload.get("channel_id") or ""),
            "name": str(payload.get("channel_name") or ""),
            "url": _http_url(payload.get("channel_url")) or "",
        },
        "thumbnail_url": _http_url(payload.get("thumbnail_url")) or (
            f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
            if video_id and VIDEO_ID.fullmatch(video_id) else None
        ),
        "video_url": None,
        "duration_text": str(payload.get("duration_text") or ""),
        "views_text": str(payload.get("views_text") or ""),
        "published_text": str(payload.get("published_text") or ""),
        "observed_at": _timestamp(row.get("observed_at")),
        "detail_capture": {
            "status": (
                status if status in {"captured", "failed"} else "disabled"
            ),
        },
    }


async def _discoveries(
    client: ClickHouseClient, ids: list[str], filters: YouTubeFilters,
) -> dict[str, list[dict[str, Any]]]:
    # Return observations from this feed first, with bounded history per item.
    result = await client.fetch_json(
        "SELECT item_id, feed_url, feed_name, feed_kind, tab, shelf, rank, "
        "region, playlist_id, observed_at, run_id "
        "FROM youtube_discoveries FINAL "
        "WHERE item_id IN %(ids)s "
        "ORDER BY (feed_url = %(feed)s) DESC, observed_at DESC, "
        "if(feed_kind != 'hype' AND "
        "JSONExtractUInt(payload, 'discovery', 'position') > 0, "
        "JSONExtractUInt(payload, 'discovery', 'position'), rank) ASC "
        "LIMIT %(limit)s BY item_id",
        {"ids": ids, "feed": filters.feed, "limit": MAX_ITEM_DISCOVERIES},
    )
    grouped: dict[str, list[dict[str, Any]]] = {}

    for row in result.rows:
        value = {key: str(row.get(key) or "") for key in (
            "feed_url", "feed_name", "feed_kind", "tab", "shelf", "region",
            "playlist_id", "run_id",
        )}
        observation: dict[str, Any] = {
            **value, "observed_at": _timestamp(row.get("observed_at")),
            "rank": int(row.get("rank") or 0) or None,
        }
        grouped.setdefault(str(row["item_id"]), []).append(observation)

    return grouped


async def _references(
    client: ClickHouseClient, ids: list[str],
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    video_ids = [item.removeprefix("video:") for item in ids
                 if item.startswith("video:")]

    if not video_ids:
        return {}, {}

    result = await client.fetch_json(
        "SELECT l.video_id AS video_id, l.domain AS domain, "
        "l.post_id AS post_id, "
        "argMax(l.url, l.observed_at) AS url, "
        "argMax(l.source, l.observed_at) AS source, "
        "argMaxIf(if(empty(p.title), l.title, p.title), l.observed_at, "
        "notEmpty(p.title) OR notEmpty(l.title)) AS title, "
        "max(notEmpty(p.domain)) AS processed, "
        "max(observed_at) AS latest_at, count() OVER (PARTITION BY video_id) "
        "AS reference_count FROM youtube_links AS l LEFT JOIN "
        "(SELECT domain, post_id, title FROM posts FINAL "
        "WHERE (domain, post_id) IN "
        "(SELECT domain, post_id FROM youtube_links "
        "WHERE video_id IN %(ids)s)) AS p "
        "ON l.domain = p.domain AND l.post_id = p.post_id "
        "WHERE l.video_id IN %(ids)s "
        "GROUP BY video_id, domain, post_id ORDER BY latest_at DESC "
        "LIMIT %(limit)s BY video_id",
        {"ids": video_ids, "limit": MAX_ITEM_REFERENCES},
    )
    grouped: dict[str, list[dict[str, Any]]] = {}
    counts: dict[str, int] = {}

    for row in result.rows:
        item_id = "video:" + str(row["video_id"])
        domain, post_id = str(row["domain"]), str(row["post_id"])
        reference = {
            "domain": domain, "post_id": post_id,
            "url": _http_url(row.get("url")) or "",
            "title": str(row.get("title") or ""),
            "source": str(row.get("source") or ""),
            "reader_url": (
                "/reader/?" + urlencode({
                    "article_source": domain, "article": post_id,
                }) if row.get("processed") else ""
            ),
        }
        grouped.setdefault(item_id, []).append(reference)
        counts[item_id] = int(row.get("reference_count") or 0)

    return grouped, counts


async def handle_youtube_sources(request: web.Request) -> web.Response:
    client: ClickHouseClient = request.app["clickhouse"]
    result = await client.fetch_json(
        "SELECT feed_url, argMax(feed_name, observed_at) AS name, "
        "argMax(feed_kind, observed_at) AS kind, "
        "argMax(region, observed_at) AS region, "
        "argMax(tab, observed_at) AS tab, uniqExact(item_id) AS count, "
        "argMax(JSONExtractString(payload, 'channel_id'), observed_at) "
        "AS channel_id, "
        "argMax(JSONExtractString(payload, 'channel_name'), observed_at) "
        "AS channel_name, "
        "argMax(JSONExtractString(payload, 'channel_url'), observed_at) "
        "AS channel_url "
        "FROM youtube_discoveries WHERE feed_kind = 'channel' OR "
        "(feed_url, run_id) IN (SELECT feed_url, argMax(run_id, observed_at) "
        "FROM youtube_discoveries GROUP BY feed_url) "
        "GROUP BY feed_url ORDER BY name",
    )
    feeds, channels = configured_sources(
        request.app["state"].settings, result.rows,
    )
    await _channel_counts(client, channels)
    total = await client.fetch_json(
        "SELECT uniqExact(item_id) AS total FROM youtube_discoveries",
    )
    linked = await client.fetch_json(
        "SELECT uniqExact(video_id) AS total FROM youtube_links",
    )

    return web.json_response({
        "feeds": feeds, "channels": channels,
        "total": int(total.rows[0]["total"]) if total.rows else 0,
        "linked_count": int(linked.rows[0]["total"]) if linked.rows else 0,
    })


async def _channel_counts(
    client: ClickHouseClient, channels: list[dict[str, Any]],
) -> None:
    if not channels:
        return

    result = await client.fetch_json(
        "SELECT id, uniqExact(item_id) AS count FROM ("
        "SELECT item_id, arrayJoin(arrayIntersect(["
        "JSONExtractString(payload, 'channel_id'), "
        "JSONExtractString(payload, 'channel_url')], %(ids)s)) AS id "
        "FROM youtube_discoveries) GROUP BY id",
        {"ids": [str(channel["id"]) for channel in channels]},
    )
    counts = {str(row["id"]): int(row["count"]) for row in result.rows}

    for channel in channels:
        channel["count"] = counts.get(str(channel["id"]), 0)


def _timestamp(value: object) -> str:
    date = parse_datetime(value)

    return date.isoformat() if date else ""


def _http_url(value: object) -> str | None:
    if not isinstance(value, str):
        return None

    try:
        parsed = urlsplit(value)

        if (
            parsed.scheme in {"http", "https"} and parsed.hostname
            and not parsed.username and not parsed.password
            and parsed.port in {None, 80, 443}
        ):
            return value
    except ValueError:
        pass

    return None


async def _local_media(
    client: ClickHouseClient, ids: list[str], assets_dir: pathlib.Path,
) -> dict[str, dict[str, str]]:
    result = await client.fetch_json(
        "SELECT post_id, url, local_path, asset_type, scraped_at "
        "FROM raw_assets WHERE domain = 'youtube.com' AND post_id IN %(ids)s "
        "AND local_path != '' "
        "ORDER BY scraped_at DESC LIMIT 1 BY post_id, asset_type",
        {"ids": ids},
    )
    media: dict[str, dict[str, str]] = {}

    for row in result.rows:
        asset = _parse_asset_row({
            **row, "domain": "youtube.com", "source": "youtube",
            "content_type": "", "size_bytes": 0,
            "created_at": row.get("scraped_at"),
            "updated_at": row.get("scraped_at"),
        })
        view = _asset_view(asset, assets_dir=assets_dir)

        if view and view["kind"] in {"image", "video"}:
            key = "video_url" if view["kind"] == "video" else "thumbnail_url"
            media.setdefault(str(row["post_id"]), {})[key] = view["url"]

    return media
