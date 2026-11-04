"""What the collector adds around the scrape runner, with no network."""

from __future__ import annotations

from typing import Any

import aiohttp
import pytest

from dank.api.youtube.collector import collect_limited, collection_gate
from dank.config import Settings
from dank.scrape.http import current_limiter

VIDEO = "NnV_cWeoo5Q"


async def test_collection_runs_under_its_limiter_and_restores_the_last(
    settings: Settings,
) -> None:
    gate = collection_gate(settings.scrape)
    seen: list[object] = []

    async def collect(
        http: aiohttp.ClientSession, video_id: str,
    ) -> dict[str, Any]:
        seen.append(current_limiter.get())

        return {"video_id": video_id}

    assert await collect_limited(gate.limiter, collect, VIDEO) == {
        "video_id": VIDEO,
    }
    assert seen == [gate.limiter]
    assert current_limiter.get() is None


async def test_a_failed_collection_restores_the_last_limiter(
    settings: Settings,
) -> None:
    gate = collection_gate(settings.scrape)

    async def collect(
        http: aiohttp.ClientSession, video_id: str,
    ) -> dict[str, Any]:
        raise OSError("The network is down")

    with pytest.raises(OSError, match="network"):
        await collect_limited(gate.limiter, collect, VIDEO)

    assert current_limiter.get() is None


def test_each_gate_has_its_own_lock_and_limiter(settings: Settings) -> None:
    metadata = collection_gate(settings.scrape)
    transcript = collection_gate(settings.scrape)

    assert metadata.lock is not transcript.lock
    assert metadata.limiter is not transcript.limiter
