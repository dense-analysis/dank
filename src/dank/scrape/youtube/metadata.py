"""Small public YouTube previews, independent of feed appearances/downloads."""

from __future__ import annotations

import asyncio
import datetime as dt
import json
from typing import Any
from urllib.parse import urlencode, urlsplit

import aiohttp

from dank.scrape.http import HostCooldownError, http_response
from dank.youtube import VIDEO_ID, payload_object

# A preview is a small JSON document; do not accept arbitrary watch-page HTML.
MAX_METADATA_BYTES = 64 * 1024
METADATA_TIMEOUT_SECONDS = 12
# Local refresh policy; failed lookups must not repeat on every page read.
METADATA_TTL = dt.timedelta(days=7)
METADATA_FAILURE_TTL = dt.timedelta(hours=1)
METADATA_UNAVAILABLE_TTL = dt.timedelta(days=1)


async def fetch_video_metadata(
    client: aiohttp.ClientSession, video_id: str,
) -> dict[str, Any]:
    if not VIDEO_ID.fullmatch(video_id):
        raise ValueError("Invalid YouTube video ID")

    # oEmbed supplies attribution and preview metadata without media downloads.
    # https://oembed.com/#section2
    request_url = "https://www.youtube.com/oembed?" + urlencode({
        "url": f"https://www.youtube.com/watch?v={video_id}", "format": "json",
    })
    observed_at = dt.datetime.now(dt.UTC)
    body = b""
    http_status = 0
    status, error, metadata = "failed", "", {}

    try:
        async with asyncio.timeout(METADATA_TIMEOUT_SECONDS):
            async with http_response(client, request_url) as response:
                http_status = response.status
                body = await _preview_body(response)

                if http_status in {401, 403, 404, 410}:
                    status = "unavailable"
                    error = f"YouTube preview unavailable (HTTP {http_status})"
                else:
                    response.raise_for_status()
                    metadata = _metadata(payload_object(json.loads(body)))
                    status = "captured"
    except (
        aiohttp.ClientError, TimeoutError, ValueError, HostCooldownError,
    ) as exc:
        error = str(exc) or type(exc).__name__

    ttl = {
        "captured": METADATA_TTL, "unavailable": METADATA_UNAVAILABLE_TTL,
    }.get(status, METADATA_FAILURE_TTL)
    retry_after = observed_at + ttl
    capture: dict[str, Any] = {
        "status": status, "provider": "youtube_oembed",
        "observed_at": observed_at.isoformat(),
        "retry_after": retry_after.isoformat(),
    }

    if status == "captured":
        capture.update(available=True, captured_at=observed_at.isoformat())

    return {
        "video_id": video_id, "observed_at": observed_at,
        "retry_after": retry_after, "status": status,
        "provider": "youtube_oembed", "request_url": request_url,
        "http_status": http_status, "error": error[:1000],
        # Preserve the provider response verbatim, including failed responses.
        "provider_payload": body[:MAX_METADATA_BYTES].decode(
            "utf-8", errors="replace",
        ),
        "payload": json.dumps({**metadata, "metadata_capture": capture}),
    }


def _metadata(raw: dict[str, Any]) -> dict[str, str]:
    title = raw.get("title")

    if (raw.get("type") != "video" or not isinstance(title, str)
            or not title.strip()):
        raise ValueError("YouTube preview did not include a video title")

    channel_url = _provider_url(
        raw.get("author_url"), {"www.youtube.com", "youtube.com"},
    )
    channel_path = urlsplit(channel_url).path

    return {
        "title": title.strip(),
        "channel_name": str(raw.get("author_name") or ""),
        "channel_url": channel_url,
        "channel_id": (
            channel_path.removeprefix("/channel/")
            if channel_path.startswith("/channel/") else ""
        ),
        "thumbnail_url": _provider_url(raw.get("thumbnail_url"), {
            "i.ytimg.com", "img.youtube.com", "i1.ytimg.com",
            "i2.ytimg.com", "i3.ytimg.com", "i4.ytimg.com",
        }),
    }


def _provider_url(value: object, hosts: set[str]) -> str:
    if not isinstance(value, str):
        return ""

    parsed = urlsplit(value)

    return (
        value if parsed.scheme == "https" and parsed.hostname in hosts else ""
    )


async def _preview_body(response: aiohttp.ClientResponse) -> bytes:
    body = bytearray()

    async for chunk in response.content.iter_chunked(8192):
        body.extend(chunk)

        if len(body) > MAX_METADATA_BYTES:
            raise ValueError("YouTube preview exceeded the size limit")

    return bytes(body)
