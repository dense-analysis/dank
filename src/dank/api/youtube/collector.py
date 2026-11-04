"""VideoCollector over the scrape runner, which saves what it collects."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, NamedTuple

import aiohttp

from dank.api.youtube.model import (
    MetadataObservation,
    TranscriptPayload,
    timestamp_text,
    transcript_view,
)
from dank.api.youtube.service import CollectionLimits
from dank.config import ScrapeSettings
from dank.scrape.http import RequestLimiter, current_limiter
from dank.scrape.runner import (
    collect_youtube_video_metadata,
    collect_youtube_video_transcript,
)
from dank.scrape.youtube.transcripts import TRANSCRIPT_TIMEOUT_SECONDS
from dank.storage.clickhouse import ClickHouseClient

COLLECTION_LIMITS = CollectionLimits(
    metadata_wait=15,
    transcript_lock_wait=1,
    transcript_lookup=TRANSCRIPT_TIMEOUT_SECONDS + 5,
)

type Collect = Callable[
    [aiohttp.ClientSession, str], Awaitable[dict[str, Any]],
]


class CollectionGate(NamedTuple):
    """What every request collecting one kind of data shares.

    The lifespan owns one for metadata and one for transcripts, so they queue
    apart and each keeps its own cooldowns for a host that pushes back.
    """

    lock: asyncio.Lock
    limiter: RequestLimiter


def collection_gate(settings: ScrapeSettings) -> CollectionGate:
    return CollectionGate(asyncio.Lock(), RequestLimiter(settings))


class ScrapeVideoCollector:
    def __init__(
        self,
        client: ClickHouseClient,
        *,
        metadata: RequestLimiter,
        transcript: RequestLimiter,
    ) -> None:
        self._client = client
        self._metadata = metadata
        self._transcript = transcript

    async def metadata(self, video_id: str) -> MetadataObservation:
        async def collect(
            http: aiohttp.ClientSession, video: str,
        ) -> dict[str, Any]:
            return await collect_youtube_video_metadata(
                self._client, http, video,
            )

        row = await collect_limited(self._metadata, collect, video_id)

        return MetadataObservation(
            status=row["status"],
            provider=row["provider"],
            cached=row["cached"],
            observed_at=timestamp_text(row["observed_at"]),
            retry_after=timestamp_text(row["retry_after"]),
        )

    async def transcript(self, video_id: str) -> TranscriptPayload:
        async def collect(
            http: aiohttp.ClientSession, video: str,
        ) -> dict[str, Any]:
            return await collect_youtube_video_transcript(
                self._client, http, video,
            )

        row = await collect_limited(self._transcript, collect, video_id)

        return transcript_view(video_id, row)


async def collect_limited(
    limiter: RequestLimiter, collect: Collect, video_id: str,
) -> dict[str, Any]:
    # The scrape code finds its limiter through this context variable.
    token = current_limiter.set(limiter)

    try:
        async with aiohttp.ClientSession() as http:
            return await collect(http, video_id)
    finally:
        current_limiter.reset(token)
