"""Bounded on-demand previews for video identities already known to DANK."""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
from aiohttp import web

from dank.scrape.http import RequestLimiter, current_limiter
from dank.scrape.runner import collect_youtube_video_metadata
from dank.storage.clickhouse import ClickHouseClient, parse_datetime
from dank.youtube import VIDEO_ID

METADATA_LOCK = web.AppKey("youtube_metadata_lock", asyncio.Lock)


def add_metadata_routes(app: web.Application) -> None:
    app[METADATA_LOCK] = asyncio.Lock()
    app.router.add_post(
        "/api/reader/youtube/metadata/{video_id}", handle_video_metadata,
    )


async def handle_video_metadata(request: web.Request) -> web.Response:
    origin = request.headers.get("Origin")

    if (
        request.headers.get("Sec-Fetch-Site") == "cross-site"
        or (origin is not None and origin != f"{request.scheme}://{request.host}")
    ):
        return web.json_response(
            {"error": "Same-origin request required"}, status=403,
        )

    if request.content_type != "application/json":
        return web.json_response(
            {"error": "JSON request required"}, status=415,
        )

    video_id = request.match_info["video_id"]

    if not VIDEO_ID.fullmatch(video_id):
        return web.json_response(
            {"error": "Invalid YouTube video ID"}, status=400,
        )

    client: ClickHouseClient = request.app["clickhouse"]
    known = await client.fetch_json(
        "SELECT video_id FROM (SELECT video_id "
        "FROM youtube_reference_evidence "
        "WHERE video_id = %(id)s UNION ALL "
        "SELECT substring(item_id, 7) AS video_id FROM youtube_discoveries "
        "WHERE item_id = %(item_id)s) LIMIT 1",
        {"id": video_id, "item_id": f"video:{video_id}"},
    )

    if not known.rows:
        return web.json_response({"error": "Video not found"}, status=404)

    settings = request.app["state"].settings
    token = current_limiter.set(RequestLimiter(settings.scrape))

    try:
        # The lock makes simultaneous readers reuse the first persisted result.
        # Limit waiting as well as HTTP time so requests cannot accumulate.
        async with asyncio.timeout(15), request.app[METADATA_LOCK]:
            async with aiohttp.ClientSession() as http_client:
                row = await collect_youtube_video_metadata(
                    client, http_client, video_id,
                )
    except TimeoutError:
        return web.json_response(
            {"error": "Metadata collection is busy; try again shortly"},
            status=503,
        )
    finally:
        current_limiter.reset(token)

    success_at = row["observed_at"] if row["status"] == "captured" else None

    if success_at is None:
        success = await client.fetch_json(
            "SELECT observed_at FROM youtube_video_metadata "
            "WHERE video_id = %(id)s AND status = 'captured' "
            "ORDER BY observed_at DESC LIMIT 1", {"id": video_id},
        )
        success_at = success.rows[0]["observed_at"] if success.rows else None

    return web.json_response({
        "video_id": video_id, "status": row["status"],
        "provider": row["provider"], "cached": row["cached"],
        "observed_at": _timestamp(row["observed_at"]),
        "retry_after": _timestamp(row["retry_after"]),
        "available": success_at is not None,
        "captured_at": _timestamp(success_at),
    })


def _timestamp(value: Any) -> str:
    timestamp = parse_datetime(value)

    return timestamp.isoformat() if timestamp else ""
