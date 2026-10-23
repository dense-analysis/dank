"""Feed-first YouTube collection and cross-source video references."""

from __future__ import annotations

import datetime as dt
import pathlib
from typing import Any
from urllib.parse import urlencode, urlsplit

from aiohttp import web

from dank.storage.clickhouse import ClickHouseClient, parse_datetime
from dank.thumbnails import ThumbnailSignature, similar_thumbnail_signatures
from dank.web.app import (
    AssetRow,
    _asset_view,  # pyright: ignore[reportPrivateUsage]
    _parse_asset_row,  # pyright: ignore[reportPrivateUsage]
)
from dank.web.reader_youtube_index import backfill_evidence
from dank.web.reader_youtube_pairs import (
    merge_title_pairs,
    thumbnail_title_pairs,
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
# Keep reader responses bounded while the append-only archive retains all data.
MAX_ITEM_TITLE_HISTORY = 20
MAX_THUMBNAIL_CANDIDATES = 64
MAX_THUMBNAIL_VARIANTS = 8
INDEX_VERSION = 3


async def ensure_youtube_storage(client: ClickHouseClient) -> None:
    schema = pathlib.Path(__file__).parents[1] / "storage/youtube.sql"
    statements = [
        s.strip() for s in schema.read_text().split(";") if s.strip()
    ]

    for statement in statements:
        await client.execute(statement)

    complete = await client.fetch_json(
        "SELECT DISTINCT version FROM youtube_index_versions "
        "WHERE version IN (2, %(version)s)", {"version": INDEX_VERSION},
    )

    versions = {int(row["version"]) for row in complete.rows}

    if INDEX_VERSION in versions:
        return

    # Version 2 already verified all sources it supported. Only newly supported
    # publisher captures need scanning on upgrade; fresh databases scan all.
    await backfill_evidence(
        client, sources=("linked_article",) if 2 in versions else (),
    )

    await client.insert_rows("youtube_index_versions", [{
        "version": INDEX_VERSION, "completed_at": dt.datetime.now(dt.UTC),
    }])


def add_youtube_routes(app: web.Application) -> None:
    app.router.add_get("/api/reader/youtube", handle_youtube)
    app.router.add_get("/api/reader/youtube/sources", handle_youtube_sources)
    from dank.web.reader_youtube_channels import handle_youtube_channels
    from dank.web.reader_youtube_metadata import add_metadata_routes
    from dank.web.reader_youtube_transcripts import add_transcript_routes

    app.router.add_get("/api/reader/youtube/channels", handle_youtube_channels)
    add_metadata_routes(app)
    add_transcript_routes(app)


async def handle_youtube(request: web.Request) -> web.Response:
    try:
        filters = parse_youtube_filters(request.query)
    except ValueError as error:
        return web.json_response({"error": str(error)}, status=400)

    client: ClickHouseClient = request.app["clickhouse"]
    configured: tuple[str, ...] = ()

    if filters.channel_scope:
        from dank.web.reader_youtube_channels import load_configured_channels

        configured = await load_configured_channels(
            client, request.app["state"].settings,
        )

    query, params = build_youtube_query(
        filters, configured_channels=configured,
    )
    result = await client.fetch_json(query, params)
    rows = result.rows[:filters.limit]
    query, params = build_youtube_query(
        filters, count=True, configured_channels=configured,
    )
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
            int(rows[-1].get("reference_count") or 0),
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
    titles = await _title_history(client, ids)
    media = await _local_media(client, ids, assets_dir) if assets_dir else {}
    thumbnails = (
        await _thumbnail_history(client, ids, assets_dir) if assets_dir else {}
    )
    items: list[dict[str, Any]] = []

    for row in rows:
        item_id = str(row["item_id"])
        item = _item(row)
        item.update(media.get(item_id, {}))

        if variants := thumbnails.get(item_id):
            item.update(
                thumbnail_url=variants[0]["url"], thumbnail_variants=variants,
            )

        item.update(
            discoveries=discoveries.get(item_id, []),
            references=references.get(item_id, []),
            reference_count=counts.get(item_id, 0),
            title_history=titles.get(item_id, []),
        )

        if not item["title"]:
            item["title"] = "YouTube video · " + str(item["video_id"])

        items.append(item)

    return items


def _item(row: dict[str, Any]) -> dict[str, Any]:
    payload = payload_object(row.get("payload"))
    item_id = str(row["item_id"])
    is_video = item_id.startswith("video:")
    video_id = item_id.removeprefix("video:") if is_video else None
    capture = payload_object(payload.get("detail_capture"))
    status = capture.get("status")
    metadata = payload_object(payload.get("metadata_capture"))
    metadata_status = metadata.get("status")

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
        "thumbnail_variants": [],
        "video_url": None,
        "duration_text": str(payload.get("duration_text") or ""),
        "views_text": str(payload.get("views_text") or ""),
        "published_text": str(payload.get("published_text") or ""),
        "observed_at": _timestamp(row.get("observed_at")),
        "metadata_capture": {
            "status": (
                metadata_status if metadata_status in {
                    "captured", "failed", "unavailable",
                } else "disabled"
            ),
            "provider": str(metadata.get("provider") or ""),
            "observed_at": str(metadata.get("observed_at") or ""),
            "retry_after": str(metadata.get("retry_after") or ""),
            "available": metadata.get("available") is True,
            "captured_at": str(metadata.get("captured_at") or ""),
        },
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


async def _title_history(
    client: ClickHouseClient, ids: list[str],
) -> dict[str, list[dict[str, str]]]:
    video_ids = [item for item in ids if item.startswith("video:")]

    if not video_ids:
        return {}

    result = await client.fetch_json(
        "SELECT item_id, trimBoth(JSONExtractString(payload, 'title')) "
        "AS title, min(observed_at) AS first_seen, "
        "max(observed_at) AS last_seen FROM youtube_discoveries "
        "WHERE item_id IN %(ids)s AND notEmpty(title) "
        "GROUP BY item_id, title ORDER BY last_seen DESC, title ASC "
        "LIMIT %(limit)s BY item_id",
        {"ids": video_ids, "limit": MAX_ITEM_TITLE_HISTORY},
    )
    grouped: dict[str, list[dict[str, str]]] = {}

    for row in result.rows:
        grouped.setdefault(str(row["item_id"]), []).append({
            "title": str(row["title"]),
            "first_seen": _timestamp(row.get("first_seen")),
            "last_seen": _timestamp(row.get("last_seen")),
        })

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
        "l.kind AS kind, l.reference_id AS reference_id, "
        "argMax(l.reference_url, l.observed_at) AS reference_url, "
        "argMax(l.author, l.observed_at) AS author, "
        "argMax(l.matched_url, l.observed_at) AS matched_url, "
        "argMax(l.excerpt, l.observed_at) AS excerpt, "
        "argMax(l.url, l.observed_at) AS url, "
        "argMax(l.source, l.observed_at) AS source, "
        "argMaxIf(if(empty(p.title), l.title, p.title), l.observed_at, "
        "notEmpty(p.title) OR notEmpty(l.title)) AS title, "
        "max(notEmpty(p.domain)) AS processed, "
        "max(observed_at) AS latest_at, count() OVER (PARTITION BY video_id) "
        "AS reference_count FROM youtube_reference_evidence AS l FINAL "
        "LEFT JOIN "
        "(SELECT domain, post_id, title FROM posts FINAL "
        "WHERE (domain, post_id) IN "
        "(SELECT domain, post_id FROM youtube_reference_evidence FINAL "
        "WHERE video_id IN %(ids)s)) AS p "
        "ON l.domain = p.domain AND l.post_id = p.post_id "
        "WHERE l.video_id IN %(ids)s "
        "GROUP BY video_id, domain, post_id, kind, reference_id "
        "ORDER BY latest_at DESC, domain, post_id, kind, reference_id "
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
            "kind": str(row.get("kind") or "post"),
            "reference_id": str(row.get("reference_id") or ""),
            "reference_url": _http_url(row.get("reference_url")) or "",
            "author": str(row.get("author") or ""),
            "matched_url": _http_url(row.get("matched_url")) or "",
            "excerpt": str(row.get("excerpt") or ""),
            "observed_at": _timestamp(row.get("latest_at")),
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
    state = request.app["state"]
    metadata = await _channel_metadata(client, state.assets_dir)
    feeds, channels = configured_sources(
        state.settings, result.rows, metadata,
    )
    await _channel_counts(client, channels)
    total = await client.fetch_json(
        "SELECT uniqExact(item_id) AS total FROM youtube_discoveries",
    )
    linked = await client.fetch_json(
        "SELECT uniqExact(video_id) AS total "
        "FROM youtube_reference_evidence FINAL",
    )

    return web.json_response({
        "feeds": feeds, "channels": channels,
        "total": int(total.rows[0]["total"]) if total.rows else 0,
        "linked_count": int(linked.rows[0]["total"]) if linked.rows else 0,
    })


async def _channel_metadata(
    client: ClickHouseClient, assets_dir: pathlib.Path,
) -> list[dict[str, Any]]:
    result = await client.fetch_json(
        "SELECT channel_id, channel_name, channel_url, feed_url, "
        "icon_url, local_path, content_type, reported_video_count, "
        "reported_video_count_text FROM youtube_channels FINAL",
    )

    for row in result.rows:
        if row["reported_video_count"] is not None:
            row["reported_video_count"] = int(row["reported_video_count"])

        asset = AssetRow(
            post_id="", url=str(row["icon_url"]),
            local_path=str(row["local_path"]),
            content_type=str(row["content_type"]), size_bytes=0,
        )
        view = _asset_view(asset, assets_dir=assets_dir)
        row["avatar_url"] = (
            view["url"] if view and view["kind"] == "image" else None
        )

    return result.rows


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
        "FROM youtube_discoveries WHERE startsWith(item_id, 'video:')) "
        "GROUP BY id",
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

        if view is None:
            continue

        # Raw downloads may have extensionless cache names. Their recorded
        # discovery type identifies presentation without inventing a MIME type.
        kind = {"image": "image", "photo": "image", "video": "video",
                "youtube": "video", "animated_gif": "video"}.get(
            str(row.get("asset_type")), view["kind"],
        )

        if kind in {"image", "video"}:
            key = "video_url" if kind == "video" else "thumbnail_url"
            media.setdefault(str(row["post_id"]), {})[key] = view["url"]

    return media


async def _thumbnail_history(
    client: ClickHouseClient, ids: list[str], assets_dir: pathlib.Path,
) -> dict[str, list[dict[str, Any]]]:
    video_ids = [item for item in ids if item.startswith("video:")]

    if not video_ids:
        return {}

    # Aggregate before bounding so repeated captures do not crowd out older
    # artwork or lose each content hash's first and last observation dates.
    result = await client.fetch_json(
        "SELECT d.post_id AS post_id, d.content_hash AS content_hash, "
        "argMax(d.local_path, d.observed_at) AS local_path, "
        "argMax(d.visual_hash, d.observed_at) AS visual_hash, "
        "argMax(d.visual_color, d.observed_at) AS visual_color, "
        "argMax(d.width, d.observed_at) AS width, "
        "argMax(d.height, d.observed_at) AS height, "
        "min(d.observed_at) AS first_seen, max(d.observed_at) AS last_seen "
        "FROM youtube_thumbnail_observations AS d WHERE d.post_id IN %(ids)s "
        "AND d.local_path != '' AND d.content_hash != '' "
        "GROUP BY d.post_id, d.content_hash "
        "ORDER BY last_seen DESC, content_hash ASC "
        "LIMIT %(limit)s BY post_id",
        {"ids": video_ids, "limit": MAX_THUMBNAIL_CANDIDATES},
    )
    pairs = await thumbnail_title_pairs(client, video_ids, [
        str(row["content_hash"]) for row in result.rows
        if row.get("content_hash")
    ])
    groups: dict[str, list[tuple[ThumbnailSignature, dict[str, Any]]]] = {}

    for row in result.rows:
        asset = AssetRow(
            post_id=str(row["post_id"]), url="",
            local_path=str(row["local_path"]),
            content_type="", size_bytes=0,
        )
        view = _asset_view(asset, assets_dir=assets_dir)

        if view is None:
            continue

        signature = ThumbnailSignature(
            str(row.get("visual_hash") or ""),
            str(row.get("visual_color") or ""),
            int(row.get("width") or 0), int(row.get("height") or 0),
        )
        value: dict[str, Any] = {
            "url": view["url"],
            "first_seen": _timestamp(row.get("first_seen")),
            "last_seen": _timestamp(row.get("last_seen")),
            "title_pairs": pairs.get(
                (str(row["post_id"]), str(row["content_hash"])), [],
            ),
        }
        variants = groups.setdefault(str(row["post_id"]), [])
        same = next((saved for fingerprint, saved in variants
                     if similar_thumbnail_signatures(signature, fingerprint)),
                    None)

        if same is not None:
            same["first_seen"] = min(same["first_seen"], value["first_seen"])
            same["last_seen"] = max(same["last_seen"], value["last_seen"])
            same["title_pairs"] = merge_title_pairs(
                same["title_pairs"], value["title_pairs"],
            )
        else:
            variants.append((signature, value))

    return {
        item_id: [value for _, value in variants[:MAX_THUMBNAIL_VARIANTS]]
        for item_id, variants in groups.items()
    }
