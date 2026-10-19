"""Retrieve public captions without downloading or transcribing audio."""

from __future__ import annotations

import asyncio
import datetime as dt
import html
import json
import math
import os
import shutil
import signal
import sys
from contextlib import nullcontext
from typing import Any, NamedTuple, cast
from urllib.parse import parse_qs, urlsplit

import aiohttp
from yarl import URL

from dank.scrape.http import HostCooldownError, current_limiter
from dank.youtube import VIDEO_ID, payload_object

TRANSCRIPT_TIMEOUT_SECONDS = 40
MAX_TRANSCRIPT_BYTES = 4 * 1024 * 1024
MAX_METADATA_BYTES = 8 * 1024 * 1024
MAX_SEGMENTS = 20000
FAILURE_TTL = dt.timedelta(hours=1)
UNAVAILABLE_TTL = dt.timedelta(days=1)


class CaptionTrack(NamedTuple):
    language: str
    name: str
    generated: bool
    url: str
    raw: dict[str, Any]


def select_caption_track(metadata: dict[str, Any]) -> CaptionTrack | None:
    """Prefer English, then original language; prefer manual for each."""
    candidates: list[CaptionTrack] = []

    for field, generated in (
        ("subtitles", False),
        ("automatic_captions", True),
    ):
        for language, formats in payload_object(metadata.get(field)).items():
            if language == "live_chat" or not isinstance(formats, list):
                continue

            for value in cast(list[Any], formats):
                track = payload_object(value)
                url = str(track.get("url") or "")

                if track.get("ext") != "json3" or not _caption_url(url):
                    continue

                # yt-dlp also exposes machine translations as caption tracks.
                if "tlang" in parse_qs(urlsplit(url).query):
                    continue

                candidates.append(
                    CaptionTrack(
                        language.removesuffix("-orig"),
                        str(track.get("name") or language),
                        generated,
                        url,
                        track,
                    ),
                )
                break

    original = str(metadata.get("language") or "").split("-")[0]

    return min(
        candidates,
        key=lambda track: (
            0
            if track.language.split("-")[0] == "en"
            else 1
            if track.language.split("-")[0] == original
            else 2,
            track.generated,
            track.language,
        ),
        default=None,
    )


def _caption_url(url: str) -> bool:
    try:
        parsed = urlsplit(url)

        return (
            parsed.scheme == "https"
            and parsed.hostname in {"www.youtube.com", "youtube.com"}
            and parsed.path == "/api/timedtext"
            and parsed.port in {None, 443}
            and not parsed.username
            and not parsed.password
        )
    except ValueError:
        return False


def parse_caption_segments(body: bytes) -> list[dict[str, Any]]:
    raw = payload_object(json.loads(body))
    events = raw.get("events")

    if (
        not isinstance(events, list)
        or len(cast(list[Any], events)) > MAX_SEGMENTS
    ):
        raise ValueError("Invalid or oversized caption events")

    segments: list[dict[str, Any]] = []

    for event in cast(list[Any], events):
        event = payload_object(event)
        parts = event.get("segs")

        if not isinstance(parts, list):
            continue

        text = " ".join(
            html.unescape(
                "".join(
                    str(payload_object(part).get("utf8") or "")
                    for part in cast(list[Any], parts)
                ),
            ).split(),
        )

        if not text:
            continue

        start = _milliseconds(event.get("tStartMs"))
        duration = _milliseconds(event.get("dDurationMs", 0))
        segment = {"start": start, "duration": duration, "text": text}

        if not segments or segment != segments[-1]:
            segments.append(segment)

    if not segments:
        raise ValueError("The caption track contained no readable text")

    return segments


def _milliseconds(value: Any) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        raise ValueError("Invalid caption timing")

    return value / 1000


async def _read_bounded(
    stream: asyncio.StreamReader | aiohttp.StreamReader,
    maximum: int,
) -> bytes:
    body = bytearray()

    while chunk := await stream.read(65536):
        body.extend(chunk)

        if len(body) > maximum:
            raise ValueError("Caption response exceeded the size limit")

    return bytes(body)


async def extract_caption_metadata(video_id: str) -> dict[str, Any]:
    # A subprocess can be killed on timeout, unlike a cancelled worker thread.
    # yt-dlp's print option simulates extraction and writes no media or files.
    # https://github.com/yt-dlp/yt-dlp#verbosity-and-simulation-options
    arguments = [
        sys.executable,
        "-m",
        "yt_dlp",
        "--ignore-config",
        "--no-cache-dir",
        "--no-playlist",
        "--skip-download",
        "--no-warnings",
        "--socket-timeout",
        "10",
        "--retries",
        "0",
        "--extractor-retries",
        "0",
        "--ignore-no-formats-error",
        "--extractor-args",
        "youtube:skip=hls,dash,translated_subs",
        "--print",
        "%(.{id,subtitles,automatic_captions,language,live_status})j",
    ]

    if shutil.which("node"):
        arguments.extend(["--js-runtimes", "node"])

    arguments.append(f"https://www.youtube.com/watch?v={video_id}")
    process = await asyncio.create_subprocess_exec(
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=sys.platform != "win32",
    )
    assert process.stdout is not None and process.stderr is not None

    try:
        async with asyncio.TaskGroup() as group:
            output = group.create_task(
                _read_bounded(
                    process.stdout,
                    MAX_METADATA_BYTES,
                ),
            )
            errors = group.create_task(_read_bounded(process.stderr, 65536))

        await process.wait()

        if process.returncode:
            raise ValueError(
                "YouTube caption discovery failed: "
                + errors.result().decode("utf-8", errors="replace")[-1000:],
            )

        metadata = payload_object(json.loads(output.result()))

        if metadata.get("id") != video_id:
            raise ValueError("YouTube returned a different video identity")

        if metadata.get("live_status") in {"is_live", "is_upcoming"}:
            raise ValueError("Captions are not collected for live broadcasts")

        return metadata
    finally:
        if process.returncode is None:
            try:
                if sys.platform == "win32":
                    process.kill()
                else:
                    os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass

        # Drain pipe buffers after termination so wait cannot deadlock.
        await process.communicate()


async def fetch_video_transcript(
    client: aiohttp.ClientSession,
    video_id: str,
) -> dict[str, Any]:
    if not VIDEO_ID.fullmatch(video_id):
        raise ValueError("Invalid YouTube video ID")

    observed = dt.datetime.now(dt.UTC)
    row: dict[str, Any] = {
        "video_id": video_id,
        "observed_at": observed,
        "retry_after": observed + FAILURE_TTL,
        "status": "failed",
        "error": "",
        "track_payload": "",
        "provider_payload": "",
        "payload": "{}",
    }
    limiter = current_limiter.get()
    watch_url = f"https://www.youtube.com/watch?v={video_id}"

    try:
        async with asyncio.timeout(TRANSCRIPT_TIMEOUT_SECONDS):
            async with limiter.slot(watch_url) if limiter else nullcontext():
                metadata = await extract_caption_metadata(video_id)

            row["track_payload"] = json.dumps(metadata)
            track = select_caption_track(metadata)

            if track is None:
                row.update(
                    status="unavailable",
                    retry_after=observed + UNAVAILABLE_TTL,
                )

                return row

            async with limiter.slot(track.url) if limiter else nullcontext():
                # Reject redirects and preserve signed query bytes.
                async with client.get(
                    URL(track.url, encoded=True),
                    allow_redirects=False,
                ) as response:
                    body = await _read_bounded(
                        response.content,
                        MAX_TRANSCRIPT_BYTES,
                    )
                    row["provider_payload"] = body.decode(
                        "utf-8",
                        errors="replace",
                    )

                    if response.status != 200:
                        raise ValueError(
                            "YouTube caption request failed "
                            f"(HTTP {response.status})",
                        )

            segments = parse_caption_segments(body)
            row.update(
                status="captured",
                payload=json.dumps(
                    {
                        "language": track.language,
                        "language_name": track.name,
                        "generated": track.generated,
                        "segments": segments,
                    },
                ),
            )
    except (
        aiohttp.ClientError,
        TimeoutError,
        ValueError,
        OSError,
        HostCooldownError,
        ExceptionGroup,
    ) as error:
        row["error"] = (str(error) or type(error).__name__)[:1000]

    return row
