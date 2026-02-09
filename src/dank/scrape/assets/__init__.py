from __future__ import annotations

import asyncio
import datetime
import pathlib
from collections.abc import Iterable

import aiohttp

from dank.model import AssetDiscovery, RawAsset

from .audio_video import download_audio_video_asset
from .http import download_file_http

SKIP_ASSET_TYPES = {"iframe", "link"}


async def download_assets(
    discoveries: Iterable[AssetDiscovery],
    *,
    assets_dir: pathlib.Path,
    browser_profile_dir: pathlib.Path | None = None,
    http_client: aiohttp.ClientSession,
    max_asset_bytes: int | None = None,
    concurrency: int = 4,
    scraped_at: datetime.datetime | None = None,
) -> list[RawAsset]:
    timestamp = scraped_at or datetime.datetime.now(datetime.UTC)
    unique: dict[str, AssetDiscovery] = {}

    for discovery in discoveries:
        if discovery.url:
            unique.setdefault(discovery.url, discovery)

    semaphore = asyncio.Semaphore(concurrency)

    async def _download(discovery: AssetDiscovery) -> RawAsset | None:
        target_dir = assets_dir / discovery.domain / discovery.post_id

        if discovery.asset_type in SKIP_ASSET_TYPES:
            return RawAsset(
                domain=discovery.domain,
                post_id=discovery.post_id,
                url=discovery.url,
                asset_type=discovery.asset_type,
                scraped_at=timestamp,
                source=discovery.source,
                local_path="",
            )
        elif discovery.asset_type == "youtube":
            # Download YouTube assets with yt-dlp.
            async with semaphore:
                return await download_audio_video_asset(
                    discovery=discovery,
                    target_dir=target_dir,
                    browser_profile_dir=browser_profile_dir,
                    max_asset_bytes=max_asset_bytes,
                    timestamp=timestamp,
                )
        else:
            async with semaphore:
                # Default to downloading assets with the HTTP client.
                return await download_file_http(
                    discovery=discovery,
                    target_dir=target_dir,
                    http_client=http_client,
                    max_asset_bytes=max_asset_bytes,
                    timestamp=timestamp,
                )

    results = await asyncio.gather(
        *(_download(discovery) for discovery in unique.values()),
    )

    return [result for result in results if result is not None]
