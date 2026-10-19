"""Read saved captions and collect a known video when opened."""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp
from aiohttp import web

from dank.scrape.http import RequestLimiter, current_limiter
from dank.scrape.runner import collect_youtube_video_transcript
from dank.scrape.youtube.transcripts import TRANSCRIPT_TIMEOUT_SECONDS
from dank.storage.clickhouse import ClickHouseClient, parse_datetime
from dank.youtube import VIDEO_ID, payload_object

TRANSCRIPT_LOCK = web.AppKey("youtube_transcript_lock", asyncio.Lock)
TRANSCRIPT_LIMITER = web.AppKey("youtube_transcript_limiter", RequestLimiter)


def add_transcript_routes(app: web.Application) -> None:
    app[TRANSCRIPT_LOCK] = asyncio.Lock()
    app[TRANSCRIPT_LIMITER] = RequestLimiter(app["state"].settings.scrape)
    path = "/api/reader/youtube/transcript/{video_id}"
    app.router.add_get(path, handle_video_transcript)
    app.router.add_post(path, handle_video_transcript)


async def handle_video_transcript(request: web.Request) -> web.Response:
    video_id = request.match_info["video_id"]

    if not VIDEO_ID.fullmatch(video_id):
        return web.json_response(
            {"error": "Invalid YouTube video ID"},
            status=400,
        )

    if request.method == "POST":
        origin = request.headers.get("Origin")

        if request.headers.get("Sec-Fetch-Site") == "cross-site" or (
            origin is not None
            and origin != f"{request.scheme}://{request.host}"
        ):
            return web.json_response(
                {"error": "Same-origin request required"},
                status=403,
            )

        if request.content_type != "application/json":
            return web.json_response(
                {"error": "JSON request required"},
                status=415,
            )

    client: ClickHouseClient = request.app["clickhouse"]
    known = await client.fetch_json(
        "SELECT video_id FROM (SELECT video_id "
        "FROM youtube_reference_evidence "
        "WHERE video_id = %(id)s UNION ALL SELECT substring(item_id, 7) "
        "AS video_id FROM youtube_discoveries "
        "WHERE item_id = %(item_id)s) LIMIT 1",
        {"id": video_id, "item_id": f"video:{video_id}"},
    )

    if not known.rows:
        return web.json_response({"error": "Video not found"}, status=404)

    if request.method == "GET":
        saved = await client.fetch_json(
            "SELECT * FROM youtube_transcripts WHERE video_id = %(id)s "
            "ORDER BY observed_at DESC LIMIT 1",
            {"id": video_id},
        )
        row = saved.rows[0] if saved.rows else {}
    else:
        lock = request.app[TRANSCRIPT_LOCK]

        try:
            # Do not accumulate requests behind a slow provider.
            async with asyncio.timeout(1):
                await lock.acquire()
        except TimeoutError:
            return web.json_response(
                {"error": "Transcript collection is busy; try again shortly"},
                status=503,
            )

        token = current_limiter.set(request.app[TRANSCRIPT_LIMITER])

        try:
            async with asyncio.timeout(TRANSCRIPT_TIMEOUT_SECONDS + 5):
                async with aiohttp.ClientSession() as http:
                    row = await collect_youtube_video_transcript(
                        client,
                        http,
                        video_id,
                    )
        except TimeoutError:
            return web.json_response(
                {"error": "Transcript lookup timed out; try again shortly"},
                status=503,
            )
        finally:
            current_limiter.reset(token)
            lock.release()

    return web.json_response(transcript_view(video_id, row))


def transcript_view(video_id: str, row: dict[str, Any]) -> dict[str, Any]:
    payload = payload_object(row.get("payload"))
    observed = parse_datetime(row.get("observed_at"))
    retry = parse_datetime(row.get("retry_after"))
    captured = row.get("status") == "captured"

    return {
        "video_id": video_id,
        "status": row.get("status", "not_collected"),
        "observed_at": observed.isoformat() if observed else "",
        "retry_after": retry.isoformat() if retry and not captured else "",
        "language": str(payload.get("language") or ""),
        "language_name": str(payload.get("language_name") or ""),
        "generated": payload.get("generated") is True,
        "segments": payload.get("segments", []) if captured else [],
    }
